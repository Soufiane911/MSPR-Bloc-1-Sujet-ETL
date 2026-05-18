#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# ObRail Europe — Rollback Script
# =============================================================================
# Usage: ./scripts/rollback.sh [--staging] [--to-sha <sha>]
#   --staging    Rollback l'environnement de staging (docker-compose.staging.yml)
#   --to-sha     Deploie une image GHCR specifique par son SHA
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
STAGING=false
TARGET_SHA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --staging) STAGING=true; shift ;;
    --to-sha)
      if [[ $# -lt 2 ]]; then
        log_error "--to-sha requires a SHA argument"
        exit 1
      fi
      TARGET_SHA="$2"; shift 2 ;;
    *) log_error "Unknown option: $1"; exit 1 ;;
  esac
done

# Pre-flight checks
if ! command -v docker &> /dev/null; then
  log_error "Docker is not installed."
  exit 1
fi
if ! command -v docker compose &> /dev/null; then
  log_error "Docker Compose is not installed."
  exit 1
fi
if ! command -v curl &> /dev/null; then
  log_error "curl is not installed."
  exit 1
fi

if [[ "${STAGING}" == "true" ]]; then
  COMPOSE_FILE="${PROJECT_DIR}/docker-compose.staging.yml"
  ENV_FILE="${PROJECT_DIR}/.env.staging"
else
  COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
  ENV_FILE="${PROJECT_DIR}/.env"
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  log_error "Compose file not found: ${COMPOSE_FILE}"
  exit 1
fi

log_info "Starting rollback..."
log_info "Compose file: ${COMPOSE_FILE}"

# ---------------------------------------------------------------------------
# Backup current state
# ---------------------------------------------------------------------------
BACKUP_DIR="${PROJECT_DIR}/backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "${BACKUP_DIR}"

log_info "Backing up current compose config..."
docker compose -f "${COMPOSE_FILE}" config > "${BACKUP_DIR}/docker-compose.backup.yml" 2>/dev/null || true

# Backup database if possible
if docker compose -f "${COMPOSE_FILE}" ps | grep -q database; then
  log_info "Backing up database..."
  docker compose -f "${COMPOSE_FILE}" exec -T database pg_dumpall -c -U obrail > "${BACKUP_DIR}/db_dump.sql" 2>/dev/null || log_warn "DB backup failed (database may not be running)"
fi

log_ok "Backup saved to ${BACKUP_DIR}"

# ---------------------------------------------------------------------------
# Rollback steps
# ---------------------------------------------------------------------------
log_info "Step 1/4: Stopping current containers..."
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down --remove-orphans || true

log_info "Step 2/4: Preparing target images..."
if [[ -n "${TARGET_SHA}" ]]; then
  log_info "Pulling specific image version: ${TARGET_SHA}"
  export IMAGE_TAG="${TARGET_SHA}"
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" pull || log_warn "Some images could not be pulled"
else
  log_info "Using previous local images (docker image prune avoided)"
fi

log_info "Step 3/4: Starting previous version..."
if [[ -n "${TARGET_SHA}" ]]; then
  IMAGE_TAG="${TARGET_SHA}" docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d
else
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d
fi

log_info "Step 4/4: Running health checks..."
MAX_ATTEMPTS=30
ATTEMPT=1
API_HEALTHY=false

while [[ ${ATTEMPT} -le ${MAX_ATTEMPTS} ]]; do
  if curl -sf http://localhost:8000/health &> /dev/null; then
    API_HEALTHY=true
    break
  fi
  echo -n "."
  sleep 2
  ATTEMPT=$((ATTEMPT + 1))
done

echo ""

if [[ "${API_HEALTHY}" != "true" ]]; then
  log_error "API health check failed after rollback."
  log_info "Recent logs:"
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail=50 api || true
  log_warn "You may need to restore the database manually from: ${BACKUP_DIR}/db_dump.sql"
  exit 1
fi

log_ok "Rollback complete! API is healthy."
log_info "Backup location: ${BACKUP_DIR}"

# ---------------------------------------------------------------------------
# Post-rollback notes
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo -e "${GREEN}  Rollback successful!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Verify application functionality: curl http://localhost:8000/health"
echo "  2. Check Grafana dashboards: http://localhost:3000"
echo "  3. Review logs: docker compose -f ${COMPOSE_FILE} logs -f"
echo "  4. If DB restore is needed:"
echo "     docker compose -f ${COMPOSE_FILE} exec -T database psql -U obrail < ${BACKUP_DIR}/db_dump.sql"
echo ""
echo "To return to latest version:"
echo "  ./scripts/deploy-staging.sh --pull   (for staging)"
echo "  docker compose -f ${COMPOSE_FILE} pull && docker compose -f ${COMPOSE_FILE} up -d"
echo ""
