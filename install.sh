#!/usr/bin/env bash
# Install tak-mesh-bridge as a systemd service. Run from this directory with sudo.
set -euo pipefail

APP=/opt/tak-mesh-bridge
ETC=/etc/tak-mesh-bridge
SRC="$(cd "$(dirname "$0")" && pwd)"

if [[ $EUID -ne 0 ]]; then
    echo "run with sudo" >&2
    exit 1
fi

apt-get install -y python3-venv

id takbridge &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin takbridge
usermod -aG dialout takbridge

mkdir -p "$APP" "$ETC/certs"
install -m 0644 "$SRC/tak_mesh_bridge.py" "$APP/"
[[ -d "$APP/venv" ]] || python3 -m venv "$APP/venv"
"$APP/venv/bin/pip" install --upgrade pip meshtastic

if [[ ! -f "$ETC/config.toml" ]]; then
    install -m 0640 -g takbridge "$SRC/config.example.toml" "$ETC/config.toml"
    echo "created $ETC/config.toml - review it"
fi

# Copy the bridge's client cert from the TAK Server if it exists on this box.
TAKCERTS=/opt/tak/certs/files
for f in tak-bridge.pem tak-bridge.key ca.pem; do
    if [[ -f "$TAKCERTS/$f" && ! -f "$ETC/certs/$f" ]]; then
        cp "$TAKCERTS/$f" "$ETC/certs/$f"
        echo "copied $f from $TAKCERTS"
    fi
done
chown -R root:takbridge "$ETC"
chmod 0750 "$ETC/certs"
chmod 0640 "$ETC"/certs/* 2>/dev/null || true

# Keep ModemManager from probing the RAK4631; its probes can hang the radio.
cat > /etc/udev/rules.d/99-rak-mm-ignore.rules <<'EOF'
# Stop ModemManager probing the Meshtastic RAK4631 (can hang the radio)
ATTRS{idVendor}=="239a", ATTRS{idProduct}=="8029", ENV{ID_MM_DEVICE_IGNORE}="1"
EOF
udevadm control --reload-rules

install -m 0644 "$SRC/tak-mesh-bridge.service" /etc/systemd/system/
systemctl daemon-reload
systemctl enable tak-mesh-bridge
systemctl restart tak-mesh-bridge
echo
echo "installed. follow logs with:  journalctl -u tak-mesh-bridge -f"
