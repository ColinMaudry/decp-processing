#!/bin/bash
set -euxo pipefail
DEV=$$(ls /dev/disk/by-id/*${VOLUME_ID}* | head -1)
blkid "$$DEV" || mkfs.ext4 -L decp-data "$$DEV"
mkdir -p /data /work
echo "$$DEV /data ext4 defaults,nofail 0 2" >> /etc/fstab
mount /data
install -d -m 700 /root/.ssh
echo '${SSH_PUBLIC_KEY}' >> /root/.ssh/authorized_keys
cat > /etc/decp.env <<'EOF'
${ENV_FILE}
EOF
chmod 600 /etc/decp.env
apt-get update && apt-get install -y git rsync curl
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
git clone --depth 1 --branch ${BRANCH} https://github.com/ColinMaudry/decp-processing.git /opt/decp-processing
cd /opt/decp-processing && uv sync --frozen
cat > /etc/systemd/system/prefect-worker.service <<'EOF'
[Service]
EnvironmentFile=/etc/decp.env
WorkingDirectory=/opt/decp-processing
ExecStart=/opt/decp-processing/.venv/bin/prefect worker start --pool scaleway-${ENV}
Restart=always
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload && systemctl enable --now prefect-worker
