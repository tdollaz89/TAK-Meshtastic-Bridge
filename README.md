# tak-mesh-bridge

Bridges a TAK Server and a Meshtastic LoRa mesh so LoRa-only ATAK users (using
the Meshtastic ATAK plugin) and TAK Server users (ATAK / WinTAK, local or
remote) see each other and can chat.

| Direction | What is bridged |
|---|---|
| Mesh → TAK | PLI (position, team, role, battery), All Chat, direct messages |
| TAK → Mesh | EUD PLI (rate limited), All Chat, direct messages *to mesh users* |
| Mesh → TAK | Standalone radios' own GPS position (radios without an ATAK phone) |
| Not bridged | Markers/points, shapes, data packages, team chat rooms, TAK-to-TAK DMs |

Standalone radios use the channel's **position precision**. If it is below 32
(full), their location on TAK is blurred to a grid (13 bits ≈ 3 km) and the
CoT error circle reflects that. Set the channel's position precision to full on
radios you want placed accurately.

Each mesh user gets their own TAK Server connection. This makes them real
contacts on the server, so TAK users can DM them.

## Requirements

- Raspberry Pi OS Bookworm (64-bit) or newer, Python 3.11+
- RAK4630 on Meshtastic firmware 2.3 or newer, USB-connected (shows up as `/dev/ttyACM0`)
- Official TAK Server 5.x on the same Pi

## 1. Configure the RAK4630

The Pi's node must use the same region, modem preset and channel/PSK as the
ATAK users' radios. With the node on USB:

```bash
pip install meshtastic                     # or use /opt/tak-mesh-bridge/venv/bin/meshtastic after install
meshtastic --info                          # check firmware version, region, channels
meshtastic --set lora.region US            # your region
meshtastic --set device.role CLIENT
meshtastic --seturl '<channel URL from one of the ATAK users radios>'
```

`--seturl` copies the channel set (including the PSK) from a field radio. You
can get the URL from the Meshtastic app's QR code / share screen.

## 2. Create a TAK Server client cert for the bridge

```bash
cd /opt/tak/certs
sudo su tak -c "./makeCert.sh client tak-bridge"
```

This creates `files/tak-bridge.pem`, `files/tak-bridge.key` and uses
`files/ca.pem`. `install.sh` copies them into `/etc/tak-mesh-bridge/certs/`.

**Groups:** TAK Server only delivers traffic between users in a shared group.
If your users are in groups, put the bridge in the same ones:

```bash
sudo java -jar /opt/tak/utils/UserManager.jar certmod -g <GROUP> /opt/tak/certs/files/tak-bridge.pem
```

## 3. Install

```bash
sudo ./install.sh
sudo nano /etc/tak-mesh-bridge/config.toml     # check port, cert paths, channel
sudo systemctl restart tak-mesh-bridge
journalctl -u tak-mesh-bridge -f
```

On a healthy start you should see `mesh node: ...`, `[main] connected to TAK Server`, `bridge running`.

## 4. Test each direction

1. **Mesh → TAK:** on a LoRa-only ATAK phone, wait for a PLI or send All Chat.
   The log shows `mesh->TAK PLI LORA-1 ...` and the user appears in WinTAK.
2. **TAK → Mesh:** connect WinTAK to the server. Within about 2 minutes the log shows
   `TAK->mesh pli ...` and the WinTAK user appears on the LoRa phone.
3. **Chat:** send All Chat from WinTAK and check it reaches the LoRa phone. Reply,
   then try a DM in both directions.

## Tuning airtime

LoRa is slow, so TAK → mesh traffic is throttled. With the defaults, each TAK
user's position goes out at most every 2 minutes, at least every 10 minutes
while they're online, and in between only if they moved 50 m or more. For
10–15 TAK users that's roughly one packet every 10–15 s on the mesh. Change
the `pli_*` and `tx_min_gap_s` values in the config to adjust this.

Chat is always sent before positions. Messages longer than about 180 bytes are
split into several packets.

## Troubleshooting

- **`unhandled mesh packet port=ATAK_PLUGIN_V2`**: the ATAK plugin is sending
  the newer V2 format (zstd compressed), which this bridge doesn't decode yet.
  Save some of these log lines; they're needed to add V2 support.
- **`received compressed TAKPacket`**: the Pi's node firmware is too old to
  decompress plugin traffic. Update the RAK4630 firmware.
- **TAK users see mesh users but not the reverse (or vice versa)**: check
  TAK Server groups (step 2) and that the node's channel matches the field
  radios (step 1).
- **`connect ... failed`**: check cert paths/password and that TAK Server is
  listening on 8089 (`ss -tlnp | grep 8089`).

## Development

```bash
python3 -m venv venv && venv/bin/pip install meshtastic
venv/bin/python -m unittest discover -s tests -v
```

The tests use a fake TAK Server and a fake radio, so no hardware is needed.
