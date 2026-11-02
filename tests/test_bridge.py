"""
Offline tests: a fake TAK Server (plain TCP) and a fake Meshtastic interface.
Run with:  python -m unittest discover -s tests -v
"""

import os
import socket
import sys
import threading
import time
import unittest
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import tak_mesh_bridge as tmb  # noqa: E402
from meshtastic.protobuf import atak_pb2  # noqa: E402


class FakeTakServer:
    """Accepts connections and broadcasts every event to all *other* clients, like TAK Server."""

    def __init__(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen()
        self.port = self.sock.getsockname()[1]
        self.clients = []
        self.received = []  # (client index, Element)
        self.lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                c, _ = self.sock.accept()
            except OSError:
                return
            with self.lock:
                self.clients.append(c)
                idx = len(self.clients) - 1
            threading.Thread(target=self._read, args=(c, idx), daemon=True).start()

    def _read(self, c, idx):
        parser = tmb.CotStreamParser()
        while True:
            try:
                data = c.recv(65536)
            except OSError:
                return
            if not data:
                return
            for raw in parser.feed(data):
                ev = ET.fromstring(raw)
                if ev.get("type", "").startswith("t-x"):
                    continue
                with self.lock:
                    self.received.append((idx, ev))
                self.broadcast(raw, exclude=c)

    def broadcast(self, raw, exclude=None):
        with self.lock:
            for c in self.clients:
                if c is not exclude:
                    try:
                        c.sendall(raw)
                    except OSError:
                        pass

    def close(self):
        self.sock.close()
        for c in self.clients:
            c.close()


class FakeIface:
    def __init__(self):
        self.sent = []
        self.nodesByNum = {}

    def sendData(self, data, **kw):
        tp = atak_pb2.TAKPacket()
        tp.ParseFromString(data)
        self.sent.append((tp, kw))

    def close(self):
        pass


def wait_for(pred, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.05)
    return False


def tak_pli(uid, callsign, lat, lon, team="Cyan", role="Team Member"):
    now = tmb.utcnow()
    ev, detail = tmb._new_event(uid, "a-f-G-U-C", "m-g", lat, lon, 100, now, 60)
    detail.remove(detail.find(tmb.BRIDGE_TAG))
    ET.SubElement(detail, "contact", {"callsign": callsign, "endpoint": "*:-1:stcp"})
    ET.SubElement(detail, "__group", {"name": team, "role": role})
    ET.SubElement(detail, "status", {"battery": "77"})
    ET.SubElement(detail, "track", {"speed": "3", "course": "270"})
    return ET.tostring(ev)


def tak_chat(sender_uid, sender_cs, text, to_uid=None, to_cs=None):
    ev = tmb.mesh_chat_to_cot(sender_uid, sender_cs, text, to_uid, to_cs, 1, 2, tmb.utcnow())
    d = ev.find("detail")
    d.remove(d.find(tmb.BRIDGE_TAG))
    return ET.tostring(ev)


def mesh_packet(num, pid, tp):
    return {"from": num, "id": pid, "fromId": f"!{num:08x}",
            "decoded": {"portnum": "ATAK_PLUGIN", "payload": tp.SerializeToString()}}


def mesh_pli(uid, cs, lat, lon):
    tp = atak_pb2.TAKPacket()
    tp.contact.callsign, tp.contact.device_callsign = cs, uid
    tp.group.team = atak_pb2.Team.Value("Dark_Blue")
    tp.group.role = atak_pb2.MemberRole.Value("Medic")
    tp.status.battery = 50
    tp.pli.latitude_i, tp.pli.longitude_i = int(lat * 1e7), int(lon * 1e7)
    tp.pli.altitude = 120
    return tp


def mesh_chat(uid, cs, text, to=tmb.ALL_CHAT, to_cs=""):
    tp = atak_pb2.TAKPacket()
    tp.contact.callsign, tp.contact.device_callsign = cs, uid
    tp.chat.message, tp.chat.to = text, to
    if to_cs:
        tp.chat.to_callsign = to_cs
    return tp


class BridgeTest(unittest.TestCase):
    def setUp(self):
        self.server = FakeTakServer()
        cfg = tmb.load_config(None)
        cfg["tak"].update(host="127.0.0.1", port=self.server.port, tls=False)
        cfg["mesh"].update(tx_min_gap_s=0.05, pli_min_interval_s=1.0,
                           pli_max_interval_s=30, pli_min_move_m=50)
        self.iface = FakeIface()
        self.bridge = tmb.Bridge(cfg, iface=self.iface)
        self.bridge.start()
        self.assertTrue(wait_for(lambda: self.bridge.main.connect_count == 1))
        # an independent "ATAK on the TAK Server" client
        self.atak = socket.create_connection(("127.0.0.1", self.server.port))
        self.assertTrue(wait_for(lambda: len(self.server.clients) == 2))

    def tearDown(self):
        self.bridge.close()
        self.atak.close()
        self.server.close()

    def events_from_bridge(self, typ):
        with self.server.lock:
            return [(i, e) for i, e in self.server.received if e.get("type") == typ and i != 1]

    def mesh_sent(self, kind):
        return [tp for tp, _ in self.iface.sent if tp.WhichOneof("payload_variant") == kind]

    def test_mesh_pli_and_chat_reach_tak_on_own_connection(self):
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 1, mesh_pli("ANDROID-mesh1", "LORA-1", 35.1, -80.2)))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("a-f-G-U-C")))
        idx, ev = self.events_from_bridge("a-f-G-U-C")[0]
        self.assertNotEqual(idx, 0, "PLI should go out on the user's proxy connection, not main")
        self.assertEqual(ev.get("uid"), "ANDROID-mesh1")
        d = ev.find("detail")
        self.assertEqual(d.find("contact").get("callsign"), "LORA-1")
        self.assertEqual(d.find("__group").get("name"), "Dark Blue")
        self.assertEqual(d.find("__group").get("role"), "Medic")
        self.assertAlmostEqual(float(ev.find("point").get("lat")), 35.1, places=5)

        self.bridge.on_mesh_receive(mesh_packet(0x1234, 2, mesh_chat("ANDROID-mesh1", "LORA-1", "hello tak")))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("b-t-f")))
        _, chat = self.events_from_bridge("b-t-f")[0]
        self.assertEqual(chat.find("detail/remarks").text, "hello tak")
        self.assertEqual(chat.find("detail/__chat").get("id"), tmb.ALL_CHAT)

        # duplicate mesh packet (same from/id) is ignored
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 2, mesh_chat("ANDROID-mesh1", "LORA-1", "hello tak")))
        time.sleep(0.3)
        self.assertEqual(len(self.events_from_bridge("b-t-f")), 1)

        # and none of the bridge's own CoT echoed back onto the mesh
        time.sleep(0.3)
        self.assertEqual(self.iface.sent, [])

    def test_tak_pli_and_chat_reach_mesh_with_rate_limit(self):
        self.atak.sendall(tak_pli("ANDROID-tak1", "TAK-1", 35.0, -80.0))
        self.assertTrue(wait_for(lambda: self.mesh_sent("pli")))
        tp = self.mesh_sent("pli")[0]
        self.assertEqual(tp.contact.device_callsign, "ANDROID-tak1")
        self.assertEqual(tp.contact.callsign, "TAK-1")
        self.assertEqual(tp.pli.latitude_i, 350000000)
        self.assertEqual(tp.status.battery, 77)
        self.assertEqual(tp.pli.course, 270)

        # second PLI within min interval and without movement: suppressed
        self.atak.sendall(tak_pli("ANDROID-tak1", "TAK-1", 35.0, -80.0))
        time.sleep(1.5)
        self.assertEqual(len(self.mesh_sent("pli")), 1)
        # moved 1 km after the min interval: sent
        self.atak.sendall(tak_pli("ANDROID-tak1", "TAK-1", 35.01, -80.0))
        self.assertTrue(wait_for(lambda: len(self.mesh_sent("pli")) == 2))

        long_text = "x" * 400
        self.atak.sendall(tak_chat("ANDROID-tak1", "TAK-1", long_text))
        self.assertTrue(wait_for(lambda: len(self.mesh_sent("chat")) == 3))
        chats = self.mesh_sent("chat")
        self.assertEqual("".join(c.chat.message for c in chats), long_text)
        self.assertTrue(all(c.chat.to == tmb.ALL_CHAT for c in chats))
        self.assertTrue(all(len(c.SerializeToString()) < 233 for c in chats))

    def test_direct_messages(self):
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 1, mesh_pli("ANDROID-mesh1", "LORA-1", 35.1, -80.2)))
        self.assertTrue(wait_for(lambda: "ANDROID-mesh1" in self.bridge.proxies))

        # TAK user DMs the mesh user
        self.atak.sendall(tak_chat("ANDROID-tak1", "TAK-1", "just for you", "ANDROID-mesh1", "LORA-1"))
        self.assertTrue(wait_for(lambda: self.mesh_sent("chat")))
        tp = self.mesh_sent("chat")[0]
        self.assertEqual(tp.chat.to, "ANDROID-mesh1")
        self.assertEqual(tp.chat.to_callsign, "LORA-1")

        # TAK-to-TAK DM is not put on the mesh
        self.atak.sendall(tak_chat("ANDROID-tak1", "TAK-1", "private", "ANDROID-tak2", "TAK-2"))
        time.sleep(0.5)
        self.assertEqual(len(self.mesh_sent("chat")), 1)

        # mesh user replies by DM -> TAK event with marti dest
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 5, mesh_chat(
            "ANDROID-mesh1", "LORA-1", "roger", to="ANDROID-tak1", to_cs="TAK-1")))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("b-t-f")))
        _, ev = self.events_from_bridge("b-t-f")[0]
        self.assertEqual(ev.find("detail/marti/dest").get("callsign"), "TAK-1")
        self.assertEqual(ev.find("detail/__chat/chatgrp").get("uid1"), "ANDROID-tak1")

    def test_plugin_chat_uid_with_message_id(self):
        # Real plugin traffic: PLI uses the bare uid, chats use "<uid>|<message id>"
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 1, mesh_pli("ANDROID-mesh1", "LORA-1", 35.1, -80.2)))
        self.atak.sendall(tak_pli("S-1-5-21-wintak", "WINTAK-1", 35.0, -80.0))
        self.assertTrue(wait_for(lambda: "S-1-5-21-wintak" in self.bridge.tak_callsigns))
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 2, mesh_chat(
            "ANDROID-mesh1|1fd0f2b4-8fb9", "LORA-1", "p2p test", to="S-1-5-21-wintak")))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("b-t-f")))
        self.assertEqual(list(self.bridge.proxies), ["ANDROID-mesh1"])
        _, ev = self.events_from_bridge("b-t-f")[0]
        self.assertEqual(ev.find("detail/__chat/chatgrp").get("uid0"), "ANDROID-mesh1")
        self.assertEqual(ev.find("detail/__chat").get("messageId"), "1fd0f2b4-8fb9")
        self.assertEqual(ev.find("detail/marti/dest").get("callsign"), "WINTAK-1")

        # plugin read/delivery receipts sent as text are not shown on TAK
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 3, mesh_chat(
            "ANDROID-mesh1|x", "LORA-1", "ACK:D:787ff6aa", to="S-1-5-21-wintak")))
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 4, mesh_chat(
            "ANDROID-mesh1|y", "LORA-1", "ACK:R:787ff6aa", to="S-1-5-21-wintak")))
        time.sleep(0.5)
        self.assertEqual(len(self.events_from_bridge("b-t-f")), 1)

    def test_callsign_change_reconnects(self):
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 1, mesh_pli("ANDROID-mesh1", "OLD-CS", 35.1, -80.2)))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("a-f-G-U-C")))
        first_idx, _ = self.events_from_bridge("a-f-G-U-C")[0]
        old_conn = self.bridge.proxies["ANDROID-mesh1"].conn
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 2, mesh_pli("ANDROID-mesh1", "NEW-CS", 35.1, -80.2)))
        self.assertTrue(wait_for(lambda: len(self.events_from_bridge("a-f-G-U-C")) == 2))
        idx, ev = self.events_from_bridge("a-f-G-U-C")[1]
        self.assertNotEqual(idx, first_idx, "new callsign should arrive on a fresh TAK connection")
        self.assertEqual(ev.find("detail/contact").get("callsign"), "NEW-CS")
        self.assertIsNot(self.bridge.proxies["ANDROID-mesh1"].conn, old_conn)

    def test_standalone_node_position(self):
        from meshtastic.protobuf import mesh_pb2
        self.iface.nodesByNum[0xabcd] = {"user": {"longName": "Relay-Hill"}}
        pos = mesh_pb2.Position(latitude_i=368322000, longitude_i=-761482000,
                                altitude=30, precision_bits=13)
        pkt = {"from": 0xabcd, "id": 7, "fromId": "!0000abcd",
               "decoded": {"portnum": "POSITION_APP", "payload": pos.SerializeToString()}}
        self.bridge.on_mesh_receive(pkt)
        self.assertTrue(wait_for(lambda: self.events_from_bridge("a-f-G-E-S")))
        _, ev = self.events_from_bridge("a-f-G-E-S")[0]
        self.assertEqual(ev.get("uid"), "MESHNODE-!0000abcd")
        self.assertEqual(ev.find("detail/contact").get("callsign"), "Relay-Hill")
        self.assertAlmostEqual(float(ev.find("point").get("lat")), 36.8322, places=4)
        self.assertGreater(float(ev.find("point").get("ce")), 2000)  # 13-bit grid ~2.9 km

        # TAK can't DM a bare radio, so a "DM" to it never goes onto the mesh
        self.atak.sendall(tak_chat("ANDROID-tak1", "TAK-1", "hi radio", "MESHNODE-!0000abcd", "Relay-Hill"))
        time.sleep(0.5)
        self.assertEqual(self.mesh_sent("chat"), [])

        # no-fix position (0,0) is ignored
        empty = {"from": 0xabcd, "id": 8, "fromId": "!0000abcd",
                 "decoded": {"portnum": "POSITION_APP", "payload": mesh_pb2.Position().SerializeToString()}}
        self.bridge.on_mesh_receive(empty)
        time.sleep(0.3)
        self.assertEqual(len(self.events_from_bridge("a-f-G-E-S")), 1)

    def test_radio_with_atak_user_not_shown_twice(self):
        from meshtastic.protobuf import mesh_pb2
        pos = mesh_pb2.Position(latitude_i=368322000, longitude_i=-761482000)
        pkt = lambda pid: {"from": 0x1234, "id": pid, "fromId": "!00001234",
                           "decoded": {"portnum": "POSITION_APP", "payload": pos.SerializeToString()}}
        self.bridge.on_mesh_receive(pkt(1))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("a-f-G-E-S")))
        # the same radio then carries ATAK plugin traffic: its node icon goes away
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 2, mesh_pli("ANDROID-mesh1", "LORA-1", 35.1, -80.2)))
        self.assertNotIn("MESHNODE-!00001234", self.bridge.proxies)
        self.bridge.on_mesh_receive(pkt(3))
        time.sleep(0.3)
        self.assertNotIn("MESHNODE-!00001234", self.bridge.proxies)
        self.assertEqual(len(self.events_from_bridge("a-f-G-E-S")), 1)

    def test_echo_suppressed_even_if_server_strips_tag(self):
        self.bridge.on_mesh_receive(mesh_packet(0x1234, 1, mesh_pli("ANDROID-mesh1", "LORA-1", 35.1, -80.2)))
        self.assertTrue(wait_for(lambda: self.events_from_bridge("a-f-G-U-C")))
        _, ev = self.events_from_bridge("a-f-G-U-C")[0]
        d = ev.find("detail")
        d.remove(d.find(tmb.BRIDGE_TAG))
        self.server.broadcast(ET.tostring(ev))
        time.sleep(0.5)
        self.assertEqual(self.iface.sent, [])

    def test_dual_homed_user_not_bridged(self):
        # a phone on both the TAK Server and the mesh
        self.atak.sendall(tak_pli("ANDROID-both", "BOTH", 35.0, -80.0))
        self.assertTrue(wait_for(lambda: "ANDROID-both" in self.bridge.tak_seen))
        self.bridge.on_mesh_receive(mesh_packet(0x9999, 1, mesh_pli("ANDROID-both", "BOTH", 35.0, -80.0)))
        time.sleep(0.5)
        self.assertNotIn("ANDROID-both", self.bridge.proxies)
        self.assertEqual(self.events_from_bridge("a-f-G-U-C"), [])


class ParserTest(unittest.TestCase):
    def test_stream_split(self):
        p = tmb.CotStreamParser()
        a = b'<?xml version="1.0"?><event uid="a"><detail/></event>'
        b = b'<event uid="b"><detail/></event>'
        out = p.feed(a + b[:10])
        self.assertEqual(len(out), 1)
        out = p.feed(b[10:])
        self.assertEqual(out, [b])


if __name__ == "__main__":
    unittest.main()
