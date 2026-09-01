#!/bin/bash
# =============================================================================
# setup.sh — EcoPipeline setup & deployment
# =============================================================================
# Usage:
#   bash setup.sh                 — Full stack deploy (default, same as full-stack)
#   bash setup.sh full-stack      — Stop, rebuild, migrate, start all services
#   bash setup.sh dags-only       — Sync DAGs only (no restart needed)
#   bash setup.sh --dev           — Full stack with dev profile (reverse-proxy, mongo)
# =============================================================================
set -euo pipefail

MODE="${1:-full-stack}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[setup]${NC} $*"; }
warn() { echo -e "${YELLOW}[setup]${NC} $*"; }
err()  { echo -e "${RED}[setup]${NC} $*" >&2; }

# ── DAGs-only deploy ──────────────────────────────────────────
deploy_dags_only() {
    log "DAGs-only deploy started"

    local scheduler
    scheduler=$(docker compose ps --format '{{.Name}}' 2>/dev/null | grep scheduler || true)

    if [[ -z "$scheduler" ]]; then
        warn "Airflow scheduler is not running — DAGs are synced but won't execute until the stack is up."
        warn "Run 'bash setup.sh full-stack' to bring up all services."
    else
        log "Scheduler ($scheduler) is running — Airflow will pick up DAG changes automatically."
    fi

    log "DAGs synced to $SCRIPT_DIR/dags/"
    log "DAGs-only deploy completed"
}

# ── Full stack deploy ─────────────────────────────────────────
deploy_full_stack() {
    PROFILE=""
    # "heavy" holds the Kafka/Spark images: local dev wants them, the prod deploy
    # must never pull them (see docker-compose.yaml).
    [[ "$MODE" == "--dev" ]] && PROFILE="--profile dev --profile heavy"

    [[ ! -f ".env" ]] && { err ".env file not found. Copy env_example.txt to .env"; exit 1; }
    set -a; source .env; set +a

    if [[ ! -d "secrets/kafka" ]]; then
        warn "secrets/kafka/ directory not found — Kafka services may fail to start."
    fi

    log "Validating docker-compose configuration..."
    docker compose $PROFILE config --quiet 2>/dev/null || { err "docker-compose.yaml validation failed"; exit 1; }

    log "Stopping existing containers..."
    docker compose $PROFILE down --remove-orphans --timeout 30 || true

    log "Building and starting services..."
    docker compose $PROFILE up -d --build || { err "Failed to start services"; exit 1; }

    log "Waiting for postgres..."
    local retries=30
    until docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-airflow}" >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -eq 0 ]]; then
            err "Postgres failed to become healthy within 60 seconds"
            docker compose logs postgres --tail=20
            exit 1
        fi
        sleep 2
    done

    log "Running Airflow migrations..."
    docker compose exec -T apiserver airflow db migrate || {
        warn "Migration via apiserver failed, trying with run..."
        docker compose run --rm apiserver airflow db migrate
    }

    log "Container status:"
    docker compose ps --format "table {{.Name}}\t{{.Status}}"

    echo ""
    log "Done! Services:"
    echo "  Airflow:        http://localhost:8080"
    echo "  Control Center: http://localhost:9021"
    echo "  Spark UI:       http://localhost:9090"
    echo ""
    echo "Create admin user:"
    echo "  docker compose run --rm apiserver airflow users create \\"
    echo "    --username admin --role Admin --email admin@local --password admin"
}

# ── Main ──────────────────────────────────────────────────────
case "$MODE" in
    dags-only)
        deploy_dags_only
        ;;
    full-stack|--dev)
        deploy_full_stack
        ;;
    *)
        err "Usage: bash setup.sh {full-stack|dags-only|--dev}"
        exit 1
        ;;
esac
