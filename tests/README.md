# Monitoring Tests

This directory contains tests for verifying that the monitoring and metrics collection system is working correctly.

## Files

- **`test_monitoring.sh`** - Bash script to generate test traffic and visualize metrics
- **`test_monitoring.py`** - Pytest-based tests for monitoring components

## Frontend E2E (Playwright)

The React frontend includes an E2E test suite powered by Playwright.

### Location

- `frontend/e2e/smoke.spec.js`
- `frontend/e2e/navigation.spec.js`
- `frontend/e2e/filters.spec.js`

### Prerequisites

Start the stack first (frontend must be reachable on port 8501):

```bash
docker compose up -d
curl -I http://localhost:8501
```

### Run locally

```bash
cd frontend
npm install
npx playwright install chromium
npm run e2e
```

Optional debug modes:

```bash
npm run e2e:headed
npm run e2e:ui
```

### CI integration

Playwright E2E tests are executed in GitHub Actions after service health checks:

- workflow: `.github/workflows/python-tests.yml`
- browser: Chromium
- report artifact: `playwright-report`

## Prerequisites

Before running tests, ensure all services are running:

```bash
docker-compose up -d
```

Services should be accessible at:
- API: `http://localhost:8000`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- PostgreSQL Exporter: `http://localhost:9187`
- cAdvisor: `http://localhost:8080`

## Using the Bash Test Script

The bash script generates realistic test traffic and displays metrics.

### Quick Start

```bash
bash tests/test_monitoring.sh
```

### With Custom Parameters

```bash
# Run for 120 seconds with 5 concurrent requests
bash tests/test_monitoring.sh --duration 120 --concurrent 5

# Run with custom number of requests per endpoint
bash tests/test_monitoring.sh --requests 100
```

### What It Does

1. ✅ Checks API, Prometheus, and Grafana connectivity
2. 📊 Generates test traffic for 60 seconds (default)
3. 🧪 Tests error scenarios (404s, invalid parameters)
4. 📈 Retrieves and displays metrics snapshots
5. 🔍 Queries Prometheus for request rates

### Example Output

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ObRail Europe - Monitoring Test Suite
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[1/6] Checking API connectivity...
✓ API is reachable

[2/6] Checking Prometheus connectivity...
✓ Prometheus is healthy

[3/6] Checking Grafana connectivity...
✓ Grafana is healthy

[4/6] Generating test traffic for 60s with 3 concurrent requests...
Running tests... 60s elapsed, ~180 requests sent
✓ Test traffic generation complete
  Total requests sent: 180

[5/6] Testing error scenarios...
  Testing 404 errors (non-existent endpoint)...
  ✓ 404 error requests sent

📊 View your metrics:
  • API Metrics Endpoint: http://localhost:8000/metrics
  • Prometheus Dashboard: http://localhost:9090
  • Grafana Dashboard: http://localhost:3000/d/obrail-monitoring
```

### View Results in Grafana

After running the test script:

1. Open Grafana: http://localhost:3000/d/obrail-monitoring
2. Set dashboard time range to "Last 5 minutes"
3. Observe the following panels:
   - **HTTP Request Rate**: Should show activity during test
   - **Active Requests**: Should spike during test period
   - **HTTP Request Duration**: P95/P99 latencies
   - **Error Rate**: From 404 and invalid parameter tests

## Using the Pytest Tests

### Install Test Dependencies

```bash
pip install pytest requests
```

### Run All Tests

```bash
pytest tests/test_monitoring.py -v
```

### Run Specific Test Classes

```bash
# Test metrics collection only
pytest tests/test_monitoring.py::TestMetricsCollection -v

# Test Prometheus integration
pytest tests/test_monitoring.py::TestPrometheusIntegration -v

# Test database metrics
pytest tests/test_monitoring.py::TestDatabaseMetrics -v

# Test monitoring stack health
pytest tests/test_monitoring.py::TestMonitoringStackHealth -v
```

### Run Specific Tests

```bash
# Test if metrics endpoint is accessible
pytest tests/test_monitoring.py::TestMetricsCollection::test_metrics_endpoint_accessible -v

# Test Prometheus health
pytest tests/test_monitoring.py::TestPrometheusIntegration::test_prometheus_accessible -v
```

### Test Categories

#### TestMetricsCollection
Verifies that the API is collecting metrics properly:
- Metrics endpoint responds with 200
- Metrics are in Prometheus format
- All expected metrics are present
- Metrics increase when requests are made

#### TestPrometheusIntegration
Verifies that Prometheus can scrape and query metrics:
- Prometheus is running and healthy
- Prometheus can query API metrics
- Scrape targets are configured correctly
- API target is marked as "up"

#### TestDatabaseMetrics
Checks that PostgreSQL metrics are available:
- PostgreSQL exporter is accessible
- PostgreSQL metrics are in Prometheus
- Database connection metrics are available

#### TestContainerMetrics
Validates container metrics from cAdvisor:
- cAdvisor is running
- Container CPU metrics are available
- Container memory metrics are available

#### TestApplicationPerformance
Tests application performance indicators:
- API response times are acceptable
- Error metrics are captured
- Request latency histograms are populated

#### TestMonitoringStackHealth
Overall health check of the entire monitoring stack:
- All services are accessible
- Prometheus has connected data sources
- Targets are healthy and up

## Real-Time Monitoring During Tests

### Option 1: Terminal Dashboard (Recommended)

Run the test script and watch Grafana simultaneously:

```bash
# Terminal 1: Run tests
bash tests/test_monitoring.sh --duration 120

# Terminal 2: Open Grafana in browser
open http://localhost:3000/d/obrail-monitoring
```

### Option 2: Prometheus Graph Dashboard

Real-time query in Prometheus:

1. Go to http://localhost:9090/graph
2. Enter query: `rate(http_requests_total[1m])`
3. Click "Execute"
4. Watch the graph update in real-time

### Option 3: Prometheus Table View

1. Go to http://localhost:9090/query
2. Enter query: `http_requests_active`
3. View results

## Common Prometheus Queries for Testing

Use these queries in Prometheus (http://localhost:9090/query) to inspect your test results:

```promql
# Request rate over 5 minutes
rate(http_requests_total[5m])

# 95th percentile latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Error rate
rate(http_requests_total{status_code=~"5.."}[5m])

# Active connections to database
pg_stat_activity_count

# CPU usage per container
rate(container_cpu_usage_seconds_total[5m])

# Memory usage percentage
(container_memory_working_set_bytes / container_memory_limit_bytes) * 100

# Request distribution by status code
sum(rate(http_requests_total[5m])) by (status_code)
```

## Monitoring While Tests Run

### Live Metrics Endpoint

While testing, watch the raw metrics:

```bash
# Terminal 1: Run test
bash tests/test_monitoring.sh

# Terminal 2: Watch metrics update live
watch -n 1 'curl -s http://localhost:8000/metrics | grep http_requests_total | tail -5'
```

## Debugging Failed Tests

### If PyTest tests fail:

1. **Check services are running**
   ```bash
   docker-compose ps
   # All containers should show "Up"
   ```

2. **Check service connectivity**
   ```bash
   curl http://localhost:8000/health
   curl http://localhost:9090/-/healthy
   curl http://localhost:3000/api/health
   ```

3. **View service logs**
   ```bash
   docker-compose logs prometheus
   docker-compose logs api
   docker-compose logs postgres_exporter
   ```

4. **Verify metrics are being collected**
   ```bash
   curl http://localhost:8000/metrics | head -20
   ```

### If bash script hangs:

1. **Check API is responding**
   ```bash
   curl -v http://localhost:8000/health
   ```

2. **Reduce test duration**
   ```bash
   bash tests/test_monitoring.sh --duration 30
   ```

3. **Check Docker containers**
   ```bash
   docker-compose down
   docker-compose up -d
   sleep 30
   bash tests/test_monitoring.sh
   ```

## Integration with CI/CD

The GitHub Actions workflow (`.github/workflows/monitoring.yml`) automatically:
- Runs health checks every 10 minutes during business hours
- Executes performance tests
- Validates monitoring stack configuration
- Reports failures

## Sample Test Results

After a successful test run, you should see:

```
✓ 150-200 total requests sent
✓ 5-10 error requests for testing
✓ Metrics visible in Prometheus
✓ Dashboard panels showing activity
✓ Request rate: ~2-3 requests/second during test
✓ P95 latency: <100ms (healthy)
✓ Error rate: ~5% (from intentional errors)
```

## Performance Baseline

Healthy metrics during test:
- **Request Rate**: 5-10 req/s
- **P95 Latency**: <100ms
- **P99 Latency**: <200ms
- **Error Rate**: <5% (without intentional errors)
- **CPU Usage**: <20%
- **Memory Usage**: <30%
- **Database Connections**: 1-5 active

If your metrics differ significantly, review [MONITORING.md](../MONITORING.md) for troubleshooting.