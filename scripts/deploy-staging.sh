#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# ObRail Europe — Staging Deployment Script
# =============================================================================
# Usage: ./scripts/deploy-staging.sh [--build] [--pull]
#   --build  Force local rebuild instead of pulling GHCR images
#   --pull   Force pull latest images from GHCR (default if GHCR_TOKEN set)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.staging.yml"
ENV_FILE="${PROJECT_DIR}/.env.staging"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
BUILD_LOCAL=false
PULL_IMAGES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) BUILD_LOCAL=true; shift ;;
    --pull)  PULL_IMAGES=true; shift ;;
    *) log_error "Unknown option: $1"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
log_info "Starting ObRail Europe staging deployment..."

if ! command -v docker &> /dev/null; then
  log_error "Docker is not installed. Please install Docker first."
  exit 1
fi

if ! docker info &> /dev/null; then
  log_error "Docker daemon is not running. Please start Docker."
  exit 1
fi

if ! command -v docker compose &> /dev/null; then
  log_error "Docker Compose (v2) is not installed."
  exit 1
fi

if ! command -v curl &> /dev/null; then
  log_error "curl is not installed. Please install curl first."
  exit 1
fi

if [[ ! -f "${COMPOSE_FILE}" ]]; then
  log_error "Staging compose file not found: ${COMPOSE_FILE}"
  exit 1
fi

log_ok "Pre-flight checks passed"

# ---------------------------------------------------------------------------
# Environment file
# ---------------------------------------------------------------------------
if [[ ! -f "${ENV_FILE}" ]]; then
  log_warn "Staging env file not found: ${ENV_FILE}"
  log_info "Creating ${ENV_FILE} from .env.example..."
  if [[ -f "${PROJECT_DIR}/.env.example" ]]; then
    cp "${PROJECT_DIR}/.env.example" "${ENV_FILE}"
    log_warn "Please review and update ${ENV_FILE} with staging values"
  else
    log_error ".env.example not found. Cannot create staging env file."
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# GHCR login (optional)
# ---------------------------------------------------------------------------
if [[ "${PULL_IMAGES}" == "true" ]] || [[ -n "${GHCR_TOKEN:-}" ]]; then
  if [[ -n "${GHCR_TOKEN:-}" ]]; then
    log_info "Logging in to GHCR..."
    echo "${GHCR_TOKEN}" | docker login ghcr.io -u "${GHCR_USER:-${USER}}" --password-stdin
    log_ok "GHCR login successful"
  else
    log_warn "GHCR_TOKEN not set. Skipping GHCR login."
  fi
fi

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------
cd "${PROJECT_DIR}"

# Stop existing stack gracefully
log_info "Stopping existing staging stack (if any)..."
docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" down --remove-orphans || true

# Remove old images to force refresh (optional cleanup)
if [[ "${PULL_IMAGES}" == "true" ]]; then
  log_info "Pulling latest images from GHCR..."
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" pull
  log_ok "Images pulled successfully"
fi

# Build or start
if [[ "${BUILD_LOCAL}" == "true" ]]; then
  log_info "Building images locally..."
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d --build
else
  log_info "Starting staging stack..."
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" up -d
fi

log_ok "Staging stack started"

# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
log_info "Running health checks..."

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

echo "" # newline after dots

if [[ "${API_HEALTHY}" != "true" ]]; then
  log_error "API health check failed after ${MAX_ATTEMPTS} attempts"
  log_info "Showing recent logs:"
  docker compose -f "${COMPOSE_FILE}" --env-file "${ENV_FILE}" logs --tail=50 api || true
  exit 1
fi
log_ok "API is healthy (localhost:8000)"

# Check Grafana
if curl -sf http://localhost:3000/api/health &> /dev/null; then
  log_ok "Grafana is healthy (localhost:3000)"
else
  log_warn "Grafana not ready yet (may need more time)"
fi

# Check Prometheus
if curl -sf http://localhost:9090/-/healthy &> /dev/null; then
  log_ok "Prometheus is healthy (localhost:9090)"
else
  log_warn "Prometheus not ready yet (may need more time)"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo -e "${GREEN}  Staging deployment complete!${NC}"
echo "=========================================="
echo ""
echo "Services:"
echo "  API (FastAPI)     : http://localhost:8000"
echo "  API Docs (Swagger): http://localhost:8000/docs"
echo "  Dashboard (React) : http://localhost:8501"
echo "  Grafana           : http://localhost:3000  (cf. GRAFANA_ADMIN_PASSWORD dans .env.staging)"
echo "  Prometheus        : http://localhost:9090"
echo ""
echo "Commands:"
echo "  View logs         : docker compose -f ${COMPOSE_FILE} logs -f"
echo "  Stop stack        : docker compose -f ${COMPOSE_FILE} down"
echo "  Restart API       : docker compose -f ${COMPOSE_FILE} restart api"
echo ""
