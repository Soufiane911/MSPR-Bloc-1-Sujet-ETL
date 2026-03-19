#!/bin/bash

set -e

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"
DURATION="${DURATION:-60}"
CONCURRENT="${CONCURRENT:-3}"
REQUESTS_PER_ENDPOINT="${REQUESTS_PER_ENDPOINT:-50}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

while [[ $# -gt 0 ]]; do
  case $1 in
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --concurrent)
      CONCURRENT="$2"
      shift 2
      ;;
    --requests)
      REQUESTS_PER_ENDPOINT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--duration SECONDS] [--concurrent NUM] [--requests NUM]"
      exit 1
      ;;
  esac
done

echo -e "${BLUE}====================================================${NC}"
echo -e "${BLUE}ObRail Europe - Monitoring Test Suite${NC}"
echo -e "${BLUE}====================================================${NC}"
echo ""

echo -e "${YELLOW}[1/6] Checking API connectivity...${NC}"
if ! curl -f -s "${API_BASE_URL}/health" > /dev/null 2>&1; then
  echo -e "${RED}API is not reachable at ${API_BASE_URL}${NC}"
  echo -e "${YELLOW}Please ensure docker-compose is running: docker-compose up -d${NC}"
  exit 1
fi
echo -e "${GREEN}API is reachable${NC}"
echo ""

echo -e "${YELLOW}[2/6] Checking Prometheus connectivity...${NC}"
if ! curl -f -s "http://localhost:9090/-/healthy" > /dev/null 2>&1; then
  echo -e "${RED}Prometheus is not reachable${NC}"
  exit 1
fi
echo -e "${GREEN}Prometheus is healthy${NC}"
echo ""

echo -e "${YELLOW}[3/6] Checking Grafana connectivity...${NC}"
if ! curl -f -s "http://localhost:3000/api/health" > /dev/null 2>&1; then
  echo -e "${RED}Grafana is not reachable${NC}"
  exit 1
fi
echo -e "${GREEN}Grafana is healthy${NC}"
echo ""

echo -e "${YELLOW}[4/6] Generating test traffic for ${DURATION}s with ${CONCURRENT} concurrent requests...${NC}"
echo ""

ENDPOINTS=(
  "/trains/"
  "/stations/"
  "/operators/"
  "/schedules/"
  "/stats/operators"
  "/trains/?limit=10"
  "/stations/?limit=5"
)

make_requests() {
  local endpoint=$1
  local requests=$2
  
  for ((i=1; i<=requests; i++)); do
    curl -s "${API_BASE_URL}${endpoint}" > /dev/null &
  done
  wait
}

start_time=$(date +%s)
total_requests=0

while [ $(($(date +%s) - start_time)) -lt "$DURATION" ]; do
  endpoint=${ENDPOINTS[$RANDOM % ${#ENDPOINTS[@]}]}
  
  echo -ne "\r${BLUE}Running tests... $(($(($(date +%s) - start_time))))s elapsed, ~${total_requests} requests sent${NC}"
  
  for ((c=1; c<=CONCURRENT; c++)); do
    make_requests "$endpoint" 1 &
  done
  
  total_requests=$((total_requests + CONCURRENT))
  sleep 1
done

wait
echo ""
echo -e "${GREEN}Test traffic generation complete${NC}"
echo -e "${GREEN}  Total requests sent: ${total_requests}${NC}"
echo ""

echo -e "${YELLOW}[5/6] Testing error scenarios...${NC}"

echo -e "${BLUE}  Testing 404 errors (non-existent endpoint)...${NC}"
for i in {1..10}; do
  curl -s "${API_BASE_URL}/non-existent-endpoint-$i" > /dev/null 2>&1 &
done
wait
echo -e "${GREEN}  404 error requests sent${NC}"

echo -e "${BLUE}  Testing invalid query parameters...${NC}"
for i in {1..5}; do
  curl -s "${API_BASE_URL}/trains/?invalid_param=test&limit=abc" > /dev/null 2>&1 &
done
wait
echo -e "${GREEN}  Invalid parameter requests sent${NC}"

echo ""

echo -e "${YELLOW}[6/6] Retrieving metrics snapshot...${NC}"
echo ""

echo -e "${BLUE}API Metrics:${NC}"
echo "==================================================="
api_metrics=$(curl -s "${API_BASE_URL}/metrics" 2>/dev/null)

echo "Total HTTP Requests:"
echo "$api_metrics" | grep -E "http_requests_total" | grep -v "^#" | head -5 || echo "  (not yet available)"

echo ""
echo "Request Duration (Histogram buckets):"
echo "$api_metrics" | grep -E "http_request_duration_seconds_bucket" | grep -v "^#" | head -5 || echo "  (not yet available)"

echo ""
echo "Active Requests:"
echo "$api_metrics" | grep -E "^http_requests_active" | grep -v "^#" || echo "  (not yet available)"

echo ""

echo -e "${BLUE}Prometheus Query Results:${NC}"
echo "==================================================="

prom_total=$(curl -s 'http://localhost:9090/api/v1/query?query=http_requests_total' 2>/dev/null | jq '.data.result | length' 2>/dev/null || echo "0")
echo "Unique metric series: $prom_total"

prom_rate=$(curl -s 'http://localhost:9090/api/v1/query?query=rate(http_requests_total%5B5m%5D)' 2>/dev/null | jq '.data.result[0].value[1]' 2>/dev/null || echo "N/A")
echo "Request rate (last 5m): $prom_rate req/s"

echo ""
echo -e "${GREEN}Monitoring test suite complete${NC}"
echo ""
echo -e "${BLUE}View your metrics:${NC}"
echo "  - API Metrics Endpoint: ${API_BASE_URL}/metrics"
echo "  - Prometheus Dashboard: http://localhost:9090"
echo "  - Grafana Dashboard: http://localhost:3000/d/obrail-monitoring"
