#!/usr/bin/env bash
# Server: ~/scripts/deploy_staging_backend.sh (sync from this file after deploy).
# Syncs staging API (+ optional admin), runs migrations, restarts a single uvicorn on 8102.
set -euo pipefail

API_DIR="${STAGING_API_ROOT:-/var/www/staging-api}"
ADMIN_DIR="${STAGING_ADMIN_ROOT:-/var/www/staging-admin-app}"
GIT_BRANCH="${STAGING_GIT_BRANCH:-main}"
DEPLOY_ADMIN="${DEPLOY_STAGING_ADMIN:-1}"
RUN_SEED="${STAGING_RUN_POST_DEPLOY:-0}"
API_PORT="${STAGING_API_PORT:-8102}"

_stop_staging_api() {
  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop staging-api.service 2>/dev/null || true
  fi
  pkill -f "uvicorn main:app --host 127.0.0.1 --port ${API_PORT}" 2>/dev/null || true
  sleep 1
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${API_PORT}/tcp" 2>/dev/null || true
    sleep 1
  fi
}

_start_staging_api() {
  if systemctl start staging-api.service 2>/dev/null; then
  sleep 2
  if curl -sf "http://127.0.0.1:${API_PORT}/health" >/dev/null; then
    return 0
  fi
    echo "systemctl start succeeded but /health failed; falling back to nohup" >&2
  fi
  cd "$API_DIR"
  # shellcheck disable=SC1091
  source venv/bin/activate
  nohup uvicorn main:app --host 127.0.0.1 --port "${API_PORT}" >>/tmp/staging-api.log 2>&1 &
  sleep 2
  curl -sf "http://127.0.0.1:${API_PORT}/health" >/dev/null
}

cd "$API_DIR"
git fetch origin
git checkout "$GIT_BRANCH"
git reset --hard "origin/$GIT_BRANCH"

if [[ -f venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

pip install -q -r requirements.txt
alembic upgrade head

_stop_staging_api
_start_staging_api

if [[ "$RUN_SEED" == "1" ]]; then
  bash "$API_DIR/scripts/staging-post-deploy.sh"
fi

if [[ "$DEPLOY_ADMIN" == "1" ]]; then
  cd "$ADMIN_DIR"
  git fetch origin
  git checkout "$GIT_BRANCH"
  git reset --hard "origin/$GIT_BRANCH"
  npm ci
  npm run build
fi

echo "Staging deploy complete ($(git -C "$API_DIR" log -1 --oneline))"
