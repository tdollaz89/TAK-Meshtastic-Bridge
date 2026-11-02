#!/usr/bin/env python3
"""
tak-mesh-bridge: bridge between a TAK Server and a Meshtastic LoRa mesh.

  Mesh -> TAK : PLI and GeoChat sent by the Meshtastic ATAK plugin (port
                ATAK_PLUGIN / TAKPacket) are converted to CoT and sent to the
                TAK Server. Each mesh user gets its own TAK connection
                ("proxy") so the server sees them as real contacts and can
                route direct messages to them.
  TAK -> Mesh : EUD position reports (PLI) and GeoChat (All Chat Rooms, plus
                direct messages addressed to mesh users) are converted to
                TAKPackets and broadcast on the mesh, rate limited so the
                LoRa channel is not flooded.

Everything else (markers, data packages, video, etc.) is NOT sent onto the
mesh.
"""

import argparse
import logging
import math
import re
import signal
import socket
import ssl
import sys
import threading
import time
import tomllib
import uuid
import xml.etree.ElementTree as ET
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from google.protobuf.message import DecodeError

try:
    from meshtastic.protobuf import atak_pb2, mesh_pb2, portnums_pb2
except ImportError:  # older meshtastic python packages
    from meshtastic import atak_pb2, mesh_pb2, portnums_pb2

log = logging.getLogger("tak-mesh-bridge")

ALL_CHAT = "All Chat Rooms"
BRIDGE_TAG = "__tak_mesh_bridge"  # marker element added to every CoT we emit
MAX_CHAT_BYTES = 180              # per-packet chat text budget (LoRa MTU ~233)
ACK_RE = re.compile(r"^ACK:[DR]:")  # plugin read/delivery receipts sent as chat text

DEFAULT_CONFIG = {
    "mesh": {
        "port": "",
        "channel_index": 0,
        "tx_min_gap_s": 4.0,
        "pli_min_interval_s": 120,
        "pli_max_interval_s": 600,
        "pli_min_move_m": 50,
        "log_unknown_ports": False,
    },
    "tak": {
        "host": "127.0.0.1",
        "port": 8089,
        "tls": True,
        "cert_file": "",
        "key_file": "",
        "key_password": "",
        "ca_file": "",
        "verify_server": True,
    },
    "bridge": {
        "mesh_stale_s": 900,
        "proxy_idle_timeout_s": 1800,
        "max_proxies": 32,
        "dual_homed_window_s": 600,
        "log_level": "INFO",
        "node_positions": True,
        "node_cot_type": "a-f-G-E-S",
        "node_stale_s": 2700,
    },
}

# TAK <-> protobuf enum names
TEAM_TO_PB = {name.replace("_", " "): val for name, val in atak_pb2.Team.items()}
PB_TO_TEAM = {val: name for name, val in TEAM_TO_PB.items()}
ROLE_NAMES = {
    "Team Member": "TeamMember", "Team Lead": "TeamLead", "HQ": "HQ",
    "Sniper": "Sniper", "Medic": "Medic", "Forward Observer": "ForwardObserver",
    "RTO": "RTO", "K9": "K9",
}
ROLE_TO_PB = {tak: atak_pb2.MemberRole.Value(pb) for tak, pb in ROLE_NAMES.items()}
PB_TO_ROLE = {val: tak for tak, val in ROLE_TO_PB.items()}


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------

def utcnow():
    return datetime.now(timezone.utc)


def cot_time(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def norm_time(s):
    """Normalise a CoT timestamp so re-serialised copies compare equal."""
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except (AttributeError, ValueError):
        return s


def event_key(ev):
    return (ev.get("uid"), ev.get("type"), norm_time(ev.get("time")))


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def split_utf8(text, limit):
    """Split text into pieces of at most `limit` UTF-8 bytes on char boundaries."""
    parts, cur, size = [], [], 0
    for ch in text:
        n = len(ch.encode("utf-8"))
        if size + n > limit and cur:
            parts.append("".join(cur))
            cur, size = [], 0
        cur.append(ch)
        size += n
    if cur:
        parts.append("".join(cur))
    return parts or [""]


class SeenCache:
    """Thread-safe set of keys that expire after `ttl` seconds."""

    def __init__(self, ttl, maxlen=10000):
        self.ttl = ttl
        self.maxlen = maxlen
        self._d = OrderedDict()
        self._lock = threading.Lock()

    def _prune(self, now):
        while self._d:
            _, t = next(iter(self._d.items()))
            if now - t < self.ttl and len(self._d) <= self.maxlen:
                break
            self._d.popitem(last=False)

    def touch(self, key):
        """Mark key as seen now. Returns True if it was not already present."""
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            new = key not in self._d
            self._d[key] = now
            self._d.move_to_end(key)
            return new

    def __contains__(self, key):
        with self._lock:
            self._prune(time.monotonic())
            return key in self._d


class CotStreamParser:
    """Splits a TCP byte stream into individual <event>...</event> documents."""

    MAX_BUF = 1 << 20

    def __init__(self):
        self.buf = b""

    def feed(self, data):
        self.buf += data
        out = []
        while True:
            end = self.buf.find(b"</event>")
            if end < 0:
                break
            end += len(b"</event>")
            start = self.buf.find(b"<event")
            if 0 <= start < end:
                out.append(self.buf[start:end])
            self.buf = self.buf[end:]
        if len(self.buf) > self.MAX_BUF:
            log.warning("discarding %d bytes of unparseable TAK stream data", len(self.buf))
            self.buf = b""
        return out


# --------------------------------------------------------------------------
# CoT <-> TAKPacket conversion (pure functions)
# --------------------------------------------------------------------------

def _new_event(uid, typ, how, lat, lon, hae, now, stale_s, ce="9999999"):
    ev = ET.Element("event", {
        "version": "2.0", "uid": uid, "type": typ, "how": how,
        "time": cot_time(now), "start": cot_time(now),
        "stale": cot_time(now + timedelta(seconds=stale_s)),
    })
    ET.SubElement(ev, "point", {
        "lat": f"{lat:.7f}", "lon": f"{lon:.7f}", "hae": f"{hae:g}",
        "ce": ce, "le": "9999999",
    })
    detail = ET.SubElement(ev, "detail")
    ET.SubElement(detail, BRIDGE_TAG)
    return ev, detail


def mesh_pli_to_cot(uid, callsign, tp, now, stale_s):
    """TAKPacket with a PLI -> CoT position event (Element)."""
    pli = tp.pli
    lat, lon = pli.latitude_i / 1e7, pli.longitude_i / 1e7
    ev, detail = _new_event(uid, "a-f-G-U-C", "m-g", lat, lon, pli.altitude, now, stale_s, ce="10")
    ET.SubElement(detail, "contact", {"callsign": callsign, "endpoint": "*:-1:stcp"})
    ET.SubElement(detail, "__group", {
        "name": PB_TO_TEAM.get(tp.group.team, "Cyan") if tp.group.team else "Cyan",
        "role": PB_TO_ROLE.get(tp.group.role, "Team Member"),
    })
    if tp.status.battery:
        ET.SubElement(detail, "status", {"battery": str(tp.status.battery)})
    ET.SubElement(detail, "track", {"speed": str(pli.speed), "course": str(pli.course)})
    ET.SubElement(detail, "takv", {
        "device": "Meshtastic", "platform": "ATAK via LoRa", "os": "", "version": "tak-mesh-bridge",
    })
    ET.SubElement(detail, "uid", {"Droid": callsign})
    return ev


def mesh_chat_to_cot(sender_uid, sender_cs, message, to_uid, to_cs, lat, lon, now, msg_id=None):
    """Mesh GeoChat -> CoT b-t-f event (Element). to_uid None/ALL_CHAT = All Chat Rooms."""
    direct = bool(to_uid) and to_uid != ALL_CHAT
    room_id = to_uid if direct else ALL_CHAT
    room_name = (to_cs or to_uid) if direct else ALL_CHAT
    msg_id = msg_id or str(uuid.uuid4())
    ev, detail = _new_event(f"GeoChat.{sender_uid}.{room_id}.{msg_id}", "b-t-f", "h-g-i-g-o",
                            lat, lon, 9999999, now, 86400)
    chat = ET.SubElement(detail, "__chat", {
        "parent": "RootContactGroup", "groupOwner": "false", "messageId": msg_id,
        "chatroom": room_name, "id": room_id, "senderCallsign": sender_cs,
    })
    ET.SubElement(chat, "chatgrp", {"uid0": sender_uid, "uid1": room_id, "id": room_id})
    ET.SubElement(detail, "link", {"uid": sender_uid, "type": "a-f-G-U-C", "relation": "p-p"})
    remarks = ET.SubElement(detail, "remarks", {
        "source": f"BAO.F.ATAK.{sender_uid}", "to": room_id, "time": cot_time(now),
    })
    remarks.text = message
    if direct:
        marti = ET.SubElement(detail, "marti")
        ET.SubElement(marti, "dest", {"callsign": to_cs} if to_cs else {"uid": to_uid})
    return ev


def node_uid(num):
    return f"MESHNODE-!{num:08x}"


def mesh_position_to_cot(uid, callsign, pos, now, stale_s, cot_type):
    """Meshtastic POSITION_APP (a radio's own GPS) -> CoT event, or None if no fix."""
    if not pos.latitude_i and not pos.longitude_i:
        return None
    lat, lon = pos.latitude_i / 1e7, pos.longitude_i / 1e7
    hae = pos.altitude_hae if pos.altitude_hae else pos.altitude
    ce = "10"
    if 0 < pos.precision_bits < 32:
        # channel position precision truncates coordinates to a grid; report its radius
        ce = f"{(1 << (32 - pos.precision_bits)) * 1e-7 * 111320 / 2:.0f}"
    ev, detail = _new_event(uid, cot_type, "m-g", lat, lon, hae, now, stale_s, ce=ce)
    ET.SubElement(detail, "contact", {"callsign": callsign})
    if pos.ground_speed or pos.ground_track:
        track = pos.ground_track / 1e5 if pos.ground_track > 36000 else pos.ground_track / 100
        ET.SubElement(detail, "track", {"speed": str(pos.ground_speed), "course": f"{track:.0f}"})
    remarks = ET.SubElement(detail, "remarks")
    remarks.text = f"Meshtastic node {uid.split('-', 1)[1]}"
    return ev


def cot_to_takpackets(ev, mesh_uids):
    """
    CoT Element -> what to put on the mesh, or None if it should not be bridged.

    Returns (kind, sender_uid, [TAKPacket, ...], lat, lon) with kind "pli" or "chat".
    """
    typ = ev.get("type", "")
    uid = ev.get("uid", "")
    detail = ev.find("detail")
    point = ev.find("point")
    if detail is None:
        return None

    # --- EUD position report: an a-* event carrying a team (__group) ---
    if typ.startswith("a-") and detail.find("__group") is not None and point is not None:
        contact = detail.find("contact")
        group = detail.find("__group")
        lat, lon = _float(point.get("lat")), _float(point.get("lon"))
        hae = _float(point.get("hae"))
        tp = atak_pb2.TAKPacket()
        tp.contact.callsign = (contact.get("callsign") if contact is not None else None) or uid
        tp.contact.device_callsign = uid
        tp.group.team = TEAM_TO_PB.get(group.get("name", ""), 0)
        tp.group.role = ROLE_TO_PB.get(group.get("role", ""), 0)
        status = detail.find("status")
        if status is not None and status.get("battery"):
            tp.status.battery = max(0, min(100, int(_float(status.get("battery")))))
        tp.pli.latitude_i = int(round(lat * 1e7))
        tp.pli.longitude_i = int(round(lon * 1e7))
        tp.pli.altitude = int(hae) if abs(hae) < 100000 else 0
        track = detail.find("track")
        if track is not None:
            tp.pli.speed = max(0, int(_float(track.get("speed"))))
            tp.pli.course = int(_float(track.get("course"))) % 360
        return ("pli", uid, [tp], lat, lon)

    # --- GeoChat ---
    if typ == "b-t-f":
        chat = detail.find("__chat")
        remarks = detail.find("remarks")
        if chat is None or remarks is None or not (remarks.text or "").strip():
            return None
        grp = chat.find("chatgrp")
        link = detail.find("link")
        sender = (grp.get("uid0") if grp is not None else None) or \
                 (link.get("uid") if link is not None else None)
        if not sender:
            return None
        room_id = chat.get("id", "")
        if room_id == ALL_CHAT:
            to, to_cs = ALL_CHAT, None
        elif room_id in mesh_uids:
            to, to_cs = room_id, chat.get("chatroom")
        else:
            return None  # TAK-to-TAK DM or team/group room: stays off the mesh
        pkts = []
        for piece in split_utf8(remarks.text.strip(), MAX_CHAT_BYTES):
            tp = atak_pb2.TAKPacket()
            tp.contact.callsign = chat.get("senderCallsign") or sender
            tp.contact.device_callsign = sender
            tp.chat.message = piece
            tp.chat.to = to
            if to_cs:
                tp.chat.to_callsign = to_cs
            pkts.append(tp)
        return ("chat", sender, pkts, 0.0, 0.0)

    return None


# --------------------------------------------------------------------------
# TAK Server connection
# --------------------------------------------------------------------------

class TakConnection(threading.Thread):
    """One streaming CoT connection to the TAK Server, with auto-reconnect."""

    PING_S = 30

    def __init__(self, cfg, label, on_event, on_connect=None):
        super().__init__(daemon=True, name=f"tak-{label}")
        self.cfg = cfg
        self.label = label
        self.on_event = on_event
        self.on_connect = on_connect
        self.outq = deque(maxlen=200)
        self.connect_count = 0
        self._stop_evt = threading.Event()

    def send(self, data):
        self.outq.append(data)

    def send_first(self, data):
        self.outq.appendleft(data)

    def stop(self):
        self._stop_evt.set()

    def _connect(self):
        c = self.cfg
        sock = socket.create_connection((c["host"], int(c["port"])), timeout=10)
        if c["tls"]:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False  # TAK Server certs rarely match the hostname used
            if c["verify_server"] and c["ca_file"]:
                ctx.load_verify_locations(c["ca_file"])
            else:
                ctx.verify_mode = ssl.CERT_NONE
            if c["cert_file"]:
                ctx.load_cert_chain(c["cert_file"], c["key_file"] or None, c["key_password"] or None)
            sock = ctx.wrap_socket(sock)
        sock.settimeout(0.25)
        return sock

    def _ping(self):
        now = utcnow()
        ev, _ = _new_event("takPing", "t-x-c-t", "h-g-i-g-o", 0, 0, 0, now, 60)
        return ET.tostring(ev)

    def run(self):
        backoff = 2
        while not self._stop_evt.is_set():
            try:
                sock = self._connect()
            except Exception as e:
                log.warning("[%s] connect to %s:%s failed: %s",
                            self.label, self.cfg["host"], self.cfg["port"], e)
                self._stop_evt.wait(backoff)
                backoff = min(backoff * 2, 60)
                continue
            backoff = 2
            self.connect_count += 1
            log.info("[%s] connected to TAK Server", self.label)
            parser = CotStreamParser()
            last_ping = time.monotonic()
            try:
                if self.on_connect:
                    self.on_connect(self)
                while not self._stop_evt.is_set():
                    while self.outq:
                        sock.sendall(self.outq[0])
                        self.outq.popleft()
                    if time.monotonic() - last_ping > self.PING_S:
                        sock.sendall(self._ping())
                        last_ping = time.monotonic()
                    try:
                        data = sock.recv(65536)
                    except (socket.timeout, TimeoutError):
                        continue
                    if not data:
                        raise ConnectionError("closed by server")
                    for raw in parser.feed(data):
                        try:
                            self.on_event(self, raw)
                        except Exception:
                            log.exception("[%s] error handling TAK event", self.label)
            except Exception as e:
                if not self._stop_evt.is_set():
                    log.warning("[%s] connection lost: %s", self.label, e)
            finally:
                try:
                    sock.close()
                except Exception:
                    pass
            self._stop_evt.wait(backoff)
        log.info("[%s] stopped", self.label)


# --------------------------------------------------------------------------
# Mesh transmit scheduler
# --------------------------------------------------------------------------

class MeshTx(threading.Thread):
    """
    Rate-limited transmit queue for the LoRa side. Chats go first; PLIs are
    coalesced per user (only the newest position is kept) and only sent when
    the per-user interval / movement rules allow.
    """

    def __init__(self, send_fn, mcfg):
        super().__init__(daemon=True, name="mesh-tx")
        self.send_fn = send_fn
        self.gap = float(mcfg["tx_min_gap_s"])
        self.min_i = float(mcfg["pli_min_interval_s"])
        self.max_i = float(mcfg["pli_max_interval_s"])
        self.min_move = float(mcfg["pli_min_move_m"])
        self.chats = deque(maxlen=50)
        self.plis = OrderedDict()   # uid -> (packet, lat, lon, queued_at)
        self.last = {}              # uid -> (sent_at, lat, lon)
        self.cv = threading.Condition()
        self.last_tx = 0.0
        self.stopped = False

    def queue_chat(self, pkt):
        with self.cv:
            self.chats.append(pkt)
            self.cv.notify()

    def queue_pli(self, uid, pkt, lat, lon):
        with self.cv:
            self.plis[uid] = (pkt, lat, lon, time.monotonic())
            self.cv.notify()

    def stop(self):
        with self.cv:
            self.stopped = True
            self.cv.notify()

    def _next_pli(self, now):
        best = None
        for uid, (_, lat, lon, queued) in list(self.plis.items()):
            if now - queued > self.max_i:  # user went quiet; drop stale position
                del self.plis[uid]
                continue
            prev = self.last.get(uid)
            if prev:
                sent_at, plat, plon = prev
                age = now - sent_at
                if age < self.min_i:
                    continue
                if age < self.max_i and haversine_m(plat, plon, lat, lon) < self.min_move:
                    continue
                rank = sent_at
            else:
                rank = -1.0
            if best is None or rank < best[0]:
                best = (rank, uid)
        return best[1] if best else None

    def run(self):
        while True:
            with self.cv:
                if self.stopped:
                    return
                now = time.monotonic()
                wait = self.last_tx + self.gap - now
                pkt = None
                if wait <= 0:
                    if self.chats:
                        pkt = self.chats.popleft()
                    else:
                        uid = self._next_pli(now)
                        if uid:
                            pkt, lat, lon, _ = self.plis.pop(uid)
                            self.last[uid] = (now, lat, lon)
                if pkt is None:
                    self.cv.wait(timeout=wait if wait > 0 else 1.0)
                    continue
                self.last_tx = now
            try:
                self.send_fn(pkt)
            except Exception:
                log.exception("mesh send failed")


# --------------------------------------------------------------------------
# The bridge
# --------------------------------------------------------------------------

@dataclass
class Proxy:
    uid: str
    callsign: str
    conn: TakConnection
    last_active: float
    lat: float = 0.0
    lon: float = 0.0
    last_pli: bytes | None = None
    is_node: bool = False   # a standalone radio's own GPS, not an ATAK user


class Bridge:
    def __init__(self, cfg, iface=None):
        self.cfg = cfg
        self.mcfg, self.tcfg, self.bcfg = cfg["mesh"], cfg["tak"], cfg["bridge"]
        self.iface = iface
        self.my_node_num = None
        self.lock = threading.RLock()
        self.proxies = {}                       # mesh user uid -> Proxy
        self.tak_callsigns = {}                 # TAK user uid -> callsign (from their PLI)
        self.sent = SeenCache(900)              # CoT we sent to TAK (echo suppression)
        self.tak_dedupe = SeenCache(300)
        self.mesh_dedupe = SeenCache(900)
        self.atak_nodes = SeenCache(86400)      # radios that carry ATAK plugin traffic
        window = self.bcfg["dual_homed_window_s"]
        self.tak_seen = SeenCache(window)       # uids heard directly on TAK
        self.mesh_seen = SeenCache(window)      # uids heard directly on the mesh
        self.warned_compressed = False
        self.stopped = threading.Event()
        self.closing = False
        self.exit_code = 0
        self.tx = MeshTx(self._mesh_send, self.mcfg)
        self.main = TakConnection(self.tcfg, "main", self.on_tak_event)
        self.reaper = threading.Thread(target=self._reap_loop, daemon=True, name="reaper")

    # ---- lifecycle ----

    def start(self):
        if self.iface is None:
            self._open_mesh()
        try:
            self.my_node_num = self.iface.myInfo.my_node_num
        except AttributeError:
            pass
        self.tx.start()
        self.main.start()
        self.reaper.start()
        log.info("bridge running")

    def _open_mesh(self):
        from pubsub import pub
        from meshtastic.serial_interface import SerialInterface
        pub.subscribe(self.on_mesh_receive, "meshtastic.receive")
        pub.subscribe(self.on_mesh_lost, "meshtastic.connection.lost")
        port = self.mcfg["port"] or None
        log.info("opening Meshtastic device %s", port or "(auto-detect)")
        self.iface = SerialInterface(devPath=port)
        info = self.iface.getMyUser() or {}
        log.info("mesh node: %s (%s)", info.get("longName"), info.get("id"))

    def stop(self, code=0):
        self.exit_code = code
        self.stopped.set()

    def close(self):
        self.closing = True
        self.tx.stop()
        self.main.stop()
        with self.lock:
            for p in self.proxies.values():
                p.conn.stop()
        if self.iface is not None:
            try:
                self.iface.close()
            except Exception:
                pass

    def on_mesh_lost(self, interface=None):
        if self.closing:
            return  # we closed the device ourselves during shutdown
        log.error("lost connection to Meshtastic device; exiting so systemd can restart us")
        self.stop(1)

    # ---- proxies (one TAK connection per mesh user) ----

    def _get_proxy(self, uid, callsign, is_node=False):
        now = time.monotonic()
        with self.lock:
            p = self.proxies.get(uid)
            if p:
                p.last_active = now
                if callsign and callsign != p.callsign:
                    # TAK Server keeps the callsign it first saw on a connection,
                    # so DMs to the new callsign would not be routed. Reconnect.
                    log.info("mesh user %s is now %s; reconnecting to TAK", p.callsign, callsign)
                    p.conn.stop()
                    p.callsign = callsign
                    p.last_pli = None
                    p.conn = self._new_proxy_conn(uid, callsign)
                    p.conn.start()
                return p
            if len(self.proxies) >= self.bcfg["max_proxies"]:
                oldest = min(self.proxies.values(), key=lambda x: x.last_active)
                log.info("max proxies reached; dropping %s", oldest.callsign)
                oldest.conn.stop()
                del self.proxies[oldest.uid]
            conn = self._new_proxy_conn(uid, callsign)
            p = Proxy(uid=uid, callsign=callsign, conn=conn, last_active=now, is_node=is_node)
            self.proxies[uid] = p
            conn.start()
            log.info("new mesh user %s (%s)", callsign, uid)
            return p

    def _new_proxy_conn(self, uid, callsign):
        def on_connect(conn):
            with self.lock:
                px = self.proxies.get(uid)
                if px and px.last_pli and conn.connect_count > 1:
                    conn.send_first(px.last_pli)

        return TakConnection(self.tcfg, f"mesh:{callsign}", self.on_tak_event, on_connect)

    def _reap_loop(self):
        timeout = self.bcfg["proxy_idle_timeout_s"]
        while not self.stopped.wait(30):
            now = time.monotonic()
            with self.lock:
                for uid, p in list(self.proxies.items()):
                    if now - p.last_active > timeout:
                        log.info("mesh user %s idle; closing their TAK connection", p.callsign)
                        p.conn.stop()
                        del self.proxies[uid]

    def _send_cot(self, conn, ev):
        self.sent.touch(event_key(ev))
        data = ET.tostring(ev)
        conn.send(data)
        return data

    # ---- mesh -> TAK ----

    def on_mesh_receive(self, packet, interface=None):
        decoded = packet.get("decoded") or {}
        port = decoded.get("portnum")
        if self.my_node_num is not None and packet.get("from") == self.my_node_num:
            return
        from_num = packet.get("from") or 0
        if port in ("ATAK_PLUGIN", "ATAK_PLUGIN_V2"):
            self._mark_atak_node(from_num)
        if port == "POSITION_APP" and self.bcfg["node_positions"]:
            self.handle_mesh_position(packet, from_num)
            return
        if port != "ATAK_PLUGIN":
            if port == "ATAK_PLUGIN_V2" or self.mcfg["log_unknown_ports"]:
                payload = decoded.get("payload") or b""
                log.info("unhandled mesh packet port=%s from=%s len=%d hex=%s",
                         port, packet.get("fromId"), len(payload),
                         payload.hex() if isinstance(payload, (bytes, bytearray)) else "")
            return
        payload = decoded.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return
        tp = atak_pb2.TAKPacket()
        try:
            tp.ParseFromString(payload)
        except DecodeError:
            log.debug("undecodable TAKPacket from %s", packet.get("fromId"))
            return
        if tp.is_compressed:
            if not self.warned_compressed:
                log.warning("received compressed TAKPacket from %s; expected the node firmware "
                            "to decompress it. Check firmware version (2.3+).", packet.get("fromId"))
                self.warned_compressed = True
            return
        if not self.mesh_dedupe.touch((packet.get("from"), packet.get("id"))):
            return
        self.handle_mesh_takpacket(tp, packet.get("from") or 0)

    def _node_name(self, num):
        try:
            return self.iface.nodesByNum[num]["user"]["longName"]
        except (AttributeError, KeyError, TypeError):
            return None

    def _mark_atak_node(self, num):
        """A radio paired with an ATAK phone: its user is shown, not the radio itself."""
        self.atak_nodes.touch(num)
        uid = node_uid(num)
        with self.lock:
            p = self.proxies.pop(uid, None)
        if p:
            log.info("node %s carries ATAK traffic; no longer showing it separately", p.callsign)
            p.conn.stop()

    def handle_mesh_position(self, packet, from_num):
        if from_num in self.atak_nodes:
            return
        payload = (packet.get("decoded") or {}).get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            return
        pos = mesh_pb2.Position()
        try:
            pos.ParseFromString(payload)
        except DecodeError:
            return
        if not self.mesh_dedupe.touch((from_num, packet.get("id"))):
            return
        uid = node_uid(from_num)
        callsign = self._node_name(from_num) or packet.get("fromId") or uid
        ev = mesh_position_to_cot(uid, callsign, pos, utcnow(), self.bcfg["node_stale_s"],
                                  self.bcfg["node_cot_type"])
        if ev is None:
            return
        p = self._get_proxy(uid, callsign, is_node=True)
        p.lat, p.lon = pos.latitude_i / 1e7, pos.longitude_i / 1e7
        p.last_pli = self._send_cot(p.conn, ev)
        log.info("mesh->TAK node position %s %.5f,%.5f (precision %s bits)",
                 callsign, p.lat, p.lon, pos.precision_bits or 32)

    def handle_mesh_takpacket(self, tp, from_num):
        # The ATAK plugin sends chats as "<device uid>|<message id>"; PLIs as just the uid.
        uid, _, msg_id = tp.contact.device_callsign.partition("|")
        uid = uid or f"MESH-{from_num:08x}"
        callsign = tp.contact.callsign or self._node_name(from_num) or uid
        self.mesh_seen.touch(uid)
        if uid in self.tak_seen:
            log.debug("%s is also connected to TAK directly; not bridging", callsign)
            return
        kind = tp.WhichOneof("payload_variant")
        now = utcnow()
        if kind == "pli":
            p = self._get_proxy(uid, callsign)
            ev = mesh_pli_to_cot(uid, callsign, tp, now, self.bcfg["mesh_stale_s"])
            p.lat, p.lon = tp.pli.latitude_i / 1e7, tp.pli.longitude_i / 1e7
            p.last_pli = self._send_cot(p.conn, ev)
            log.info("mesh->TAK PLI %s %.5f,%.5f", callsign, p.lat, p.lon)
        elif kind == "chat":
            # delivery/read receipts: not bridged (newer plugins use receipt_type,
            # older ones send them as "ACK:D:<id>" / "ACK:R:<id>" text)
            if tp.chat.receipt_type or ACK_RE.match(tp.chat.message):
                log.debug("mesh chat receipt from %s not bridged", callsign)
                return
            to = tp.chat.to or ALL_CHAT
            if to != ALL_CHAT and to in self.proxies:
                return  # mesh-to-mesh DM, already delivered over LoRa
            to_cs = tp.chat.to_callsign or self.tak_callsigns.get(to)
            p = self._get_proxy(uid, callsign)
            ev = mesh_chat_to_cot(uid, callsign, tp.chat.message, to, to_cs,
                                  p.lat, p.lon, now, msg_id or None)
            self._send_cot(p.conn, ev)
            log.info("mesh->TAK chat %s -> %s: %s", callsign, to_cs or to, tp.chat.message)
        else:
            log.debug("mesh TAKPacket variant %s from %s not bridged", kind, callsign)

    # ---- TAK -> mesh ----

    def on_tak_event(self, conn, raw):
        try:
            ev = ET.fromstring(raw)
        except ET.ParseError:
            log.debug("bad CoT from TAK: %r", raw[:200])
            return
        typ, uid = ev.get("type", ""), ev.get("uid", "")
        if not uid or typ.startswith("t-x"):
            return
        key = event_key(ev)
        if key in self.sent or ev.find(f"detail/{BRIDGE_TAG}") is not None:
            return  # our own traffic echoed back
        if not self.tak_dedupe.touch(key):
            return  # same event delivered on several of our connections
        with self.lock:
            mesh_uids = set(self.proxies)
            dm_targets = {u for u, p in self.proxies.items() if not p.is_node}
        res = cot_to_takpackets(ev, dm_targets)
        if not res:
            return
        kind, sender, pkts, lat, lon = res
        if sender in mesh_uids:
            return  # a mesh user's own traffic coming back
        if kind == "pli":
            self.tak_seen.touch(sender)
            self.tak_callsigns[sender] = pkts[0].contact.callsign
        if sender in self.mesh_seen:
            return  # sender is on the mesh themselves (dual-homed)
        if kind == "pli":
            self.tx.queue_pli(sender, pkts[0], lat, lon)
        else:
            for p in pkts:
                self.tx.queue_chat(p)
            log.info("TAK->mesh chat %s -> %s: %s", pkts[0].contact.callsign,
                     pkts[0].chat.to_callsign or pkts[0].chat.to,
                     "".join(p.chat.message for p in pkts))

    def _mesh_send(self, tp):
        data = tp.SerializeToString()
        self.iface.sendData(data, destinationId="^all",
                            portNum=portnums_pb2.PortNum.ATAK_PLUGIN,
                            wantAck=False, channelIndex=int(self.mcfg["channel_index"]))
        what = tp.WhichOneof("payload_variant")
        log.info("TAK->mesh %s %s (%d bytes)", what, tp.contact.callsign, len(data))


# --------------------------------------------------------------------------

def load_config(path):
    cfg = {k: dict(v) for k, v in DEFAULT_CONFIG.items()}
    if path:
        with open(path, "rb") as f:
            user = tomllib.load(f)
        for section, values in user.items():
            cfg.setdefault(section, {}).update(values)
    return cfg


def main(argv=None):
    ap = argparse.ArgumentParser(description="TAK Server <-> Meshtastic bridge")
    ap.add_argument("-c", "--config", default="/etc/tak-mesh-bridge/config.toml")
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, str(cfg["bridge"]["log_level"]).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(threadName)s: %(message)s",
    )
    bridge = Bridge(cfg)
    signal.signal(signal.SIGTERM, lambda *_: bridge.stop(0))
    signal.signal(signal.SIGINT, lambda *_: bridge.stop(0))
    try:
        bridge.start()
    except Exception:
        log.exception("startup failed")
        return 1
    bridge.stopped.wait()
    bridge.close()
    return bridge.exit_code


if __name__ == "__main__":
    sys.exit(main())
