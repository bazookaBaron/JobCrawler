#!/usr/bin/env bash
# Idempotent installer for the LinkedIn guest crawler on an Oracle Cloud (Ubuntu/Debian) VM.
#   sudo bash setup_linkedin_crawler.sh
# Safe to re-run: it updates the code + units and never overwrites /etc/job-crawler/env.
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/bazookaBaron/JobCrawler.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/job-crawler}"
ENV_DIR=/etc/job-crawler
ENV_FILE="$ENV_DIR/env"
SVC_USER=jobcrawler
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

[ "$(id -u)" -eq 0 ] || { echo "Run as root: sudo bash $0" >&2; exit 1; }

echo "==> packages"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3 python3-venv python3-pip util-linux ca-certificates

echo "==> system user $SVC_USER"
if ! id "$SVC_USER" >/dev/null 2>&1; then
  useradd --system --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin "$SVC_USER"
fi

echo "==> code in $APP_DIR ($REPO_BRANCH)"
if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" fetch --quiet origin "$REPO_BRANCH"
  git -C "$APP_DIR" checkout --quiet "$REPO_BRANCH"
  git -C "$APP_DIR" reset --quiet --hard "origin/$REPO_BRANCH"
else
  git clone --quiet --branch "$REPO_BRANCH" "$REPO_URL" "$APP_DIR"
fi
# root owns the code (service runs read-only); the service user may only read it.
chown -R root:root "$APP_DIR"
chmod -R go+rX "$APP_DIR"

echo "==> virtualenv + minimal crawler deps (no Playwright / no heavy deps)"
[ -x "$APP_DIR/.venv/bin/python" ] || python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --quiet --upgrade pip
"$APP_DIR/.venv/bin/pip" install --quiet "asyncpg>=0.30" "httpx>=0.28" "selectolax>=0.4.11" "python-dateutil>=2.9.0"
(cd "$APP_DIR/crawler" && "$APP_DIR/.venv/bin/python" -c "import src.pgpipe.linkedin_search" ) \
  || { echo "ERROR: crawler module failed to import (is this branch the one with linkedin_search.py?)" >&2; exit 1; }

echo "==> env file $ENV_FILE"
install -d -m 750 -o root -g "$SVC_USER" "$ENV_DIR"
if [ ! -f "$ENV_FILE" ]; then
  cat > "$ENV_FILE" <<'ENVEOF'
# Postgres URI for the crawler schema (use the crawler_writer login, NOT the service-role key).
CRAWLER_DATABASE_URL=postgresql://crawler_writer:CHANGE_ME@HOST:5432/postgres?sslmode=require
# PGPIPE_SCHEMA=crawler
ENVEOF
  echo "    created placeholder - EDIT IT: sudo nano $ENV_FILE"
fi
chown "$SVC_USER":"$SVC_USER" "$ENV_FILE"
chmod 600 "$ENV_FILE"

echo "==> systemd units"
install -m 644 "$SRC_DIR/linkedin-crawl.service" /etc/systemd/system/linkedin-crawl.service
install -m 644 "$SRC_DIR/linkedin-crawl.timer"   /etc/systemd/system/linkedin-crawl.timer
systemctl daemon-reload
systemctl enable --now linkedin-crawl.timer >/dev/null

echo
systemctl list-timers linkedin-crawl.timer --no-pager || true
if grep -q CHANGE_ME "$ENV_FILE"; then
  echo "NEXT: put the real CRAWLER_DATABASE_URL in $ENV_FILE, then:"
else
  echo "NEXT:"
fi
echo "  sudo systemctl start linkedin-crawl.service     # run once now"
echo "  journalctl -u linkedin-crawl -f                 # watch it"
