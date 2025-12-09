#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Pipeline post-install script
# -----------------------------
# Usage:
#   post_install.sh
#
# Optional env vars:
#   COMPOSE_CMD=docker compose    # override if you use e.g. "docker-compose"
#   AIRFLOW_ADMIN_USER=admin
#   AIRFLOW_ADMIN_PASS=CHANGE_ME  # will be used if admin user needs creation
#   AIRFLOW_ADMIN_EMAIL=admin@yourco.com
#   AIRFLOW_ADMIN_FIRST=Admin
#   AIRFLOW_ADMIN_LAST=User
#
#   CREATE_KAFKA_SCRAM_USER=0     # set to 1 to create user below
#   KAFKA_SCRAM_USER=app1
#   KAFKA_SCRAM_PASS=CHANGE_ME
#
# Timeouts (seconds):
#   WAIT_HEALTH_TIMEOUT=180
#   WAIT_HTTP_TIMEOUT=60
#
# Notes:
# - We create placeholder secrets if they’re missing (so containers can start).
#   Replace them in ./secrets/ with real values for production.
# - Airflow DB migration is run once (no-op if already migrated).
# - Admin user creation is skipped if it already exists.

# ---------- config ----------
COMPOSE_CMD="${COMPOSE_CMD:-docker compose}"

AIRFLOW_ADMIN_USER="${AIRFLOW_ADMIN_USER:-admin}"
AIRFLOW_ADMIN_PASS="${AIRFLOW_ADMIN_PASS:-CHANGE_ME}"
AIRFLOW_ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@yourco.com}"
AIRFLOW_ADMIN_FIRST="${AIRFLOW_ADMIN_FIRST:-Admin}"
AIRFLOW_ADMIN_LAST="${AIRFLOW_ADMIN_LAST:-User}"

CREATE_KAFKA_SCRAM_USER="${CREATE_KAFKA_SCRAM_USER:-0}"
KAFKA_SCRAM_USER="${KAFKA_SCRAM_USER:-app1}"
KAFKA_SCRAM_PASS="${KAFKA_SCRAM_PASS:-CHANGE_ME}"

WAIT_HEALTH_TIMEOUT="${WAIT_HEALTH_TIMEOUT:-180}"
WAIT_HTTP_TIMEOUT="${WAIT_HTTP_TIMEOUT:-60}"

SECRETS_DIR="./secrets"
REQUIRED_SECRETS=(
  "airflow_fernet_key"
  "airflow_webserver_secret"
  "airflow_api_jwt_secret"
  "postgres_password"
  # add others you actually reference via env/.env if needed
  # "mongo_root_password"
)

# ---------- helpers ----------
info()  { printf "\033[1;34m[INFO]\033[0m %s\n" "$*"; }
warn()  { printf "\033[1;33m[WARN]\033[0m %s\n" "$*"; }
error() { printf "\033[1;31m[ERR ]\033[0m %s\n" "$*" >&2; }
die()   { error "$*"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

wait_for_health() {
  # $1 service name in compose
  # returns 0 when healthy or when container has no healthcheck (best effort)
  local svc="$1"
  local t=0
  info "Waiting for service '$svc' health (${WAIT_HEALTH_TIMEOUT}s timeout)…"
  while true; do
    # If container not created yet, continue
    if ! ${COMPOSE_CMD} ps -q "$svc" >/dev/null; then
      sleep 2; t=$((t+2))
      if [ "$t" -ge "$WAIT_HEALTH_TIMEOUT" ]; then
        warn "Timeout waiting for '$svc' to appear; continuing."
        return 0
      fi
      continue
    fi
    local cid
    cid="$(${COMPOSE_CMD} ps -q "$svc")"
    # If no health field, we can't check; consider OK
    local health
    health="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo none)"
    case "$health" in
      healthy) info "Service '$svc' is healthy."; return 0 ;;
      none)    warn "Service '$svc' has no healthcheck; skipping."; return 0 ;;
      starting) sleep 3 ;;
      unhealthy) warn "Service '$svc' reports UNHEALTHY; retrying…"; sleep 3 ;;
      *) sleep 3 ;;
    esac
    t=$((t+3))
    if [ "$t" -ge "$WAIT_HEALTH_TIMEOUT" ]; then
      warn "Timeout waiting for service '$svc' to become healthy; continuing."
      return 0
    fi
  done
}

wait_http_in_container() {
  # $1 service; $2 url; $3 optional status code (default 200..399)
  local svc="$1" url="$2" expect="${3:-up}"
  info "Waiting HTTP in '$svc' for $url (${WAIT_HTTP_TIMEOUT}s timeout)…"
  local t=0
  while true; do
    if ${COMPOSE_CMD} exec -T "$svc" sh -lc "curl -fsS -o /dev/null -w '%{http_code}' '$url'" 2>/dev/null | grep -Eq '^(2|3)[0-9]{2}$'; then
      info "HTTP OK: $svc $url"
      return 0
    fi
    sleep 2; t=$((t+2))
    if [ "$t" -ge "$WAIT_HTTP_TIMEOUT" ]; then
      warn "HTTP wait timed out for $svc $url; continuing."
      return 0
    fi
  done
}

# ---------- checks ----------
require_cmd docker
require_cmd ${COMPOSE_CMD%% *}  # first token of COMPOSE_CMD, e.g. docker

# ---------- secrets ----------
mkdir -p "$SECRETS_DIR"
for f in "${REQUIRED_SECRETS[@]}"; do
  p="$SECRETS_DIR/$f"
  if [ ! -s "$p" ]; then
    warn "Secret '$p' is missing — creating placeholder."
    # Special-case: airflow_fernet_key needs Fernet-format. If python+cryptography is present, generate real Fernet; else placeholder.
    if [ "$f" = "airflow_fernet_key" ] && command -v python >/dev/null 2>&1; then
      python - <<'PY' > "$p" || true
from base64 import urlsafe_b64encode
try:
    from cryptography.fernet import Fernet
    print(Fernet.generate_key().decode('ascii'), end='')
except Exception:
    import os
    print(urlsafe_b64encode(os.urandom(32)).decode('ascii'), end='')
PY
      if [ -s "$p" ]; then
        info "Generated Fernet key."
      else
        printf 'CHANGE_ME_FERNET_KEY' > "$p"
        warn "Could not generate Fernet key; wrote placeholder."
      fi
    else
      printf 'CHANGE_ME' > "$p"
    fi
  fi
done

# ---------- pull & up ----------
info "Pulling images…"
${COMPOSE_CMD} pull

info "Starting services (detached)…"
${COMPOSE_CMD} up -d

# ---------- wait for core services ----------
# Zookeeper -> Broker -> Schema Registry -> Connect -> Airflow -> Control Center -> Spark
for svc in zookeeper broker schema-registry connect apiserver control-center spark-master; do
  if ${COMPOSE_CMD} ps -q "$svc" >/dev/null 2>&1; then
    wait_for_health "$svc" || true
  fi
done

# Airflow API health (inside container)
if ${COMPOSE_CMD} ps -q apiserver >/dev/null 2>&1; then
  wait_http_in_container apiserver "http://localhost:8080/api/v2/monitor/health" || true
fi

# ---------- migrate Airflow DB ----------
if ${COMPOSE_CMD} ps -q apiserver >/dev/null 2>&1; then
  info "Migrating Airflow DB…"
  ${COMPOSE_CMD} run --rm apiserver airflow db migrate || warn "Airflow db migrate returned non-zero (may already be migrated)."
else
  warn "Airflow 'apiserver' service not found; skipping DB migrate."
fi

run "docker compose up -d"

# ---------- create Airflow admin (idempotent) ----------
if ${COMPOSE_CMD} ps -q apiserver >/dev/null 2>&1; then
  if ${COMPOSE_CMD} exec -T apiserver airflow users list 2>/dev/null | grep -qE "^\s*${AIRFLOW_ADMIN_USER}\s"; then
    info "Airflow user '${AIRFLOW_ADMIN_USER}' already exists. Skipping creation."
  else
    info "Creating Airflow admin user '${AIRFLOW_ADMIN_USER}'…"
    ${COMPOSE_CMD} exec -T apiserver airflow users create \
      --username "${AIRFLOW_ADMIN_USER}" \
      --firstname "${AIRFLOW_ADMIN_FIRST}" --lastname "${AIRFLOW_ADMIN_LAST}" \
      --role Admin \
      --email "${AIRFLOW_ADMIN_EMAIL}" \
      --password "${AIRFLOW_ADMIN_PASS}" || warn "Failed to create Airflow admin (may already exist)."
  fi
fi

# ---------- optional: create Kafka SCRAM user ----------
if [ "${CREATE_KAFKA_SCRAM_USER}" = "1" ]; then
  if ${COMPOSE_CMD} ps -q broker >/dev/null 2>&1; then
    info "Creating Kafka SCRAM user '${KAFKA_SCRAM_USER}'…"
    ${COMPOSE_CMD} exec -T broker bash -lc \
      "kafka-configs --bootstrap-server broker:29092 \
        --alter --add-config 'SCRAM-SHA-512=[password=${KAFKA_SCRAM_PASS}]' \
        --entity-type users --entity-name '${KAFKA_SCRAM_USER}'" \
      || warn "Kafka SCRAM user creation failed (may already exist)."
  else
    warn "Kafka 'broker' service not found; skipping SCRAM user."
  fi
fi

# ---------- quick sanity checks ----------
# Schema Registry subjects
if ${COMPOSE_CMD} ps -q schema-registry >/dev/null 2>&1; then
  info "Checking Schema Registry…"
  ${COMPOSE_CMD} exec -T schema-registry bash -lc "curl -fsS http://localhost:8081/subjects >/dev/null" \
    && info "Schema Registry OK" || warn "Schema Registry check failed."
fi

# Connect REST
if ${COMPOSE_CMD} ps -q connect >/dev/null 2>&1; then
  info "Checking Kafka Connect…"
  ${COMPOSE_CMD} exec -T connect bash -lc "curl -fsS http://localhost:8083/connectors >/dev/null" \
    && info "Connect REST OK" || warn "Connect check failed."
fi

# Control Center API (if present)
if ${COMPOSE_CMD} ps -q control-center >/dev/null 2>&1; then
  info "Checking Control Center APIs…"
  ${COMPOSE_CMD} exec -T control-center bash -lc "curl -fsS http://localhost:9021/2.0/health/status >/dev/null" \
    && info "C3 API OK" || warn "C3 API check failed."
fi

# Spark master UI
if ${COMPOSE_CMD} ps -q spark-master >/dev/null 2>&1; then
  info "Checking Spark master UI…"
  ${COMPOSE_CMD} exec -T spark-master bash -lc "curl -fsS http://localhost:8080/ >/dev/null" \
    && info "Spark UI OK" || warn "Spark UI check failed."
fi


cat <<'TXT'

------------------------------------------------------------
✅ Post-install done.

Next steps / tips:
- Replace any 'CHANGE_ME' values in ./secrets/ with real secrets.
- Airflow UI:
    - If using proxy root:      https://<your-host>/
    - If using sub-path:        https://<your-host>/airflow/
  (Inside container:            http://apiserver:8080/)
- Control Center (C3):
    - If proxied:               /c3/ plus root routes /2.0/, /3.0/, /api/, /dist/, /static/
- Spark master UI:
    - If proxied:               /spark/
- DB migrate command (manual):  docker compose run --rm apiserver airflow db migrate

Have fun! 🚀
------------------------------------------------------------
TXT
