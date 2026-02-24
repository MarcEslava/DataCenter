#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Server-side deployment script for EcoPipeline
# =============================================================================
# Usage:
#   bash deploy.sh dags-only      — Sync DAGs (no restart needed)
#   bash deploy.sh full-stack     — Full redeploy (docker compose down/up)
# =============================================================================
set -euo pipefail

MODE="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $*"; }
err()  { echo -e "${RED}[deploy]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Validations
# ---------------------------------------------------------------------------
check_prerequisites() {
    if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
        err ".env file not found in $SCRIPT_DIR"
        exit 1
    fi
    if [[ ! -f "$COMPOSE_FILE" ]]; then
        err "docker-compose.yaml not found in $SCRIPT_DIR"
        exit 1
    fi
    if ! command -v docker &>/dev/null; then
        err "docker is not installed or not in PATH"
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# DAGs-only deploy
# ---------------------------------------------------------------------------
deploy_dags_only() {
    log "DAGs-only deploy started"
    cd "$SCRIPT_DIR"

    # Verify the scheduler container is running
    local scheduler
    scheduler=$(docker compose ps --format '{{.Name}}' 2>/dev/null | grep scheduler || true)

    if [[ -z "$scheduler" ]]; then
        warn "Airflow scheduler is not running — DAGs are synced but won't execute until the stack is up."
        warn "Run 'bash deploy.sh full-stack' to bring up all services."
    else
        log "Scheduler ($scheduler) is running — Airflow will pick up DAG changes automatically."
    fi

    log "DAGs synced to $SCRIPT_DIR/dags/"
    log "DAGs-only deploy completed successfully"
}

# ---------------------------------------------------------------------------
# Full-stack deploy
# ---------------------------------------------------------------------------
deploy_full_stack() {
    log "Full-stack deploy started"
    cd "$SCRIPT_DIR"

    check_prerequisites

    # Validate secrets directory exists
    if [[ ! -d "$SCRIPT_DIR/secrets/kafka" ]]; then
        warn "secrets/kafka/ directory not found — Kafka services may fail to start."
    fi

    # Validate compose config
    log "Validating docker-compose configuration..."
    if ! docker compose config --quiet 2>/dev/null; then
        err "docker-compose.yaml validation failed"
        exit 1
    fi

    # Bring down existing stack
    log "Stopping existing containers..."
    docker compose down --remove-orphans --timeout 30 || true

    # Pull latest images (optional, won't fail if offline)
    log "Pulling latest images..."
    docker compose pull --quiet 2>/dev/null || warn "Image pull skipped (offline or images up to date)"

    # Start the stack
    log "Starting containers..."
    docker compose up -d

    # Wait for postgres to be healthy
    log "Waiting for postgres to be healthy..."
    local retries=30
    while [[ $retries -gt 0 ]]; do
        if docker compose exec -T postgres pg_isready -U airflow &>/dev/null; then
            log "Postgres is ready"
            break
        fi
        retries=$((retries - 1))
        sleep 2
    done

    if [[ $retries -eq 0 ]]; then
        err "Postgres failed to become healthy within 60 seconds"
        docker compose logs postgres --tail=20
        exit 1
    fi

    # Run Airflow DB migration
    log "Running Airflow DB migration..."
    docker compose exec -T apiserver airflow db migrate || {
        warn "DB migration via apiserver failed, trying scheduler..."
        docker compose exec -T scheduler airflow db migrate
    }

    # Quick health summary
    log "Container status:"
    docker compose ps --format "table {{.Name}}\t{{.Status}}"

    log "Full-stack deploy completed successfully"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "$MODE" in
    dags-only)
        check_prerequisites
        deploy_dags_only
        ;;
    full-stack)
        deploy_full_stack
        ;;
    *)
        err "Usage: bash deploy.sh {dags-only|full-stack}"
        exit 1
        ;;
esac
