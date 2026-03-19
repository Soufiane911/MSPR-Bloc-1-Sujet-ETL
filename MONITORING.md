# Monitoring Setup Guide for ObRail Europe

This guide explains how to set up and use monitoring for the ObRail Europe application using Prometheus, Grafana, and GitHub Workflows.

## Overview

The monitoring stack consists of:

1. **Prometheus**: Metrics collection and storage
2. **Grafana**: Visualization and dashboarding
3. **PostgreSQL Exporter**: Database metrics collection
4. **cAdvisor**: Container metrics collection
5. **FastAPI Instrumentation**: Application-level metrics

This setup monitors:
- **Application Performance**: Request rates, latencies, errors
- **Resource Utilization**: CPU, memory, disk I/O
- **Database Metrics**: Connections, query performance, table sizes

## Quick Start

### 1. Start the Monitoring Stack

```bash
docker-compose up -d
```

This will start:
- PostgreSQL database on `localhost:5432`
- FastAPI API on `localhost:8000`
- Streamlit Dashboard on `localhost:8501`
- Prometheus on `localhost:9090`
- Grafana on `localhost:3000`
- PostgreSQL Exporter on `localhost:9187`
- cAdvisor on `localhost:8080`

### 2. Access the Services

- **API Documentation**: http://localhost:8000/docs
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **API Metrics**: http://localhost:8000/metrics

### 3. Configure Grafana Data Source

Grafana should automatically discover Prometheus as a data source thanks to the provisioning files. If not:

1. Go to **Configuration** → **Data Sources**
2. Click **Add data source**
3. Select **Prometheus**
4. Set URL to `http://prometheus:9090`
5. Click **Save & Test**

### 4. View the Dashboard

1. Go to https://localhost:3000/d/obrail-monitoring
2. The dashboard displays:
   - HTTP request rates
   - Active requests
   - Request duration (p95, p99)
   - CPU and memory usage
   - PostgreSQL connections and query performance
   - HTTP status code distribution
   - API service status

## Key Metrics Being Monitored

### Application Performance

| Metric | Description |
|--------|-------------|
| `http_requests_total` | Total number of HTTP requests |
| `http_request_duration_seconds` | HTTP request latency (p50, p95, p99) |
| `http_request_size_bytes` | Size of incoming requests |
| `http_response_size_bytes` | Size of outgoing responses |
| `http_requests_active` | Currently active HTTP requests |
| `api_errors_total` | Total errors by type (client/server) |

### Resource Utilization

| Metric | Description |
|--------|-------------|
| `container_cpu_usage_seconds_total` | CPU usage per container |
| `container_memory_working_set_bytes` | Memory usage per container |
| `container_memory_limit_bytes` | Memory limit per container |
| `container_fs_usage_bytes` | Disk usage per container |
| `container_network_receive_bytes_total` | Network inbound traffic |
| `container_network_transmit_bytes_total` | Network outbound traffic |

### Database Metrics

| Metric | Description |
|--------|-------------|
| `pg_stat_activity_count` | Number of active database connections |
| `pg_stat_database_connections` | Total connections to database |
| `pg_stat_statements_mean_time` | Average query execution time |
| `pg_stat_statements_max_time` | Maximum query execution time |
| `pg_database_size_bytes` | Database size |
| `pg_table_size_bytes` | Individual table sizes |

## Creating Custom Dashboards

### 1. Create a New Dashboard

1. Go to Grafana home
2. Click **Create** → **Dashboard**
3. Click **Add new panel**

### 2. Add a Panel

Example: Monitor error rate

```
Query: rate(http_requests_total{status_code=~"5.."}[5m])
Legend: Server Errors
Type: Time series
```

### 3. Export Dashboard

To export your custom dashboard:

1. Click the dashboard settings (gear icon)
2. Select **Save as** → **Export**
3. Copy the JSON
4. Save to `grafana/provisioning/dashboards/`

## GitHub Workflow Integration

The repository includes GitHub Actions workflows for continuous monitoring:

### Monitoring Workflow (`.github/workflows/monitoring.yml`)

Runs:
- **Health checks** every 10 minutes during business hours
- **Performance tests** using Apache Bench
- **Monitoring stack validation** to ensure all components are configured correctly
- **Notifications** when issues are detected

#### What it Checks

1. **API Health**: Verifies the API is running and responding
2. **Prometheus Health**: Ensures Prometheus is collecting metrics
3. **Grafana Health**: Confirms Grafana is accessible
4. **Database Connectivity**: Tests PostgreSQL exporter connection
5. **Metrics Collection**: Verifies metrics are being collected
6. **Performance**: Runs load tests to identify bottlenecks

#### Manual Trigger

To run the workflow manually:

```bash
gh workflow run monitoring.yml
```

## Alerting Setup

### Create an Alert

1. In Grafana, open a panel
2. Click **Alert**
3. Set evaluation interval (e.g., "1m")
4. Add evaluation rules:

Example: Alert if error rate > 5%

```
WHEN avg(rate(api_errors_total[5m])) 
IS ABOVE 0.05
```

5. Configure notification channel (email, Slack, etc.)
6. Save the alert

## Troubleshooting

### Prometheus Not Collecting Metrics

1. Check Prometheus targets: http://localhost:9090/targets
2. Verify prometheus.yml is correctly formatted:
   ```bash
   docker run --rm -v $(pwd)/prometheus.yml:/prometheus.yml \
     prom/prometheus:latest promtool check config /prometheus.yml
   ```

### Grafana Shows No Data

1. Verify Prometheus data source is working
2. Test the query in Prometheus query editor directly
3. Check if scrape interval is too long (default: 15s)
4. Ensure metrics are being exposed: `curl http://localhost:8000/metrics`

### API Metrics Not Appearing

1. Restart the API container: `docker-compose restart api`
2. Verify prometheus-client is installed: `pip list | grep prometheus`
3. Check that the @app.get("/metrics") endpoint is defined
4. Verify PrometheusMiddleware is added to the FastAPI app

### Database Exporter Issues

1. Verify DATABASE_URL environment variable
2. Check PostgreSQL is running: `docker-compose logs database`
3. Test connection: `psql -h localhost -U obrail -d obrail_db`
4. Check exporter logs: `docker-compose logs postgres_exporter`

## Performance Tuning

### Adjust Scrape Intervals

Edit `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'obrail-api'
    scrape_interval: 5s
    static_configs:
      - targets: ['api:8000']
```

### Retention Policy

Prometheus stores metrics for 15 days by default. Change in `docker-compose.yml`:

```dockerfile
--storage.tsdb.retention.time=30d
```

### Disable Unnecessary Exporters

If you don't need container metrics, disable cAdvisor:

```bash
docker-compose up -d --no-start cadvisor
```

## Best Practices

1. **Set Up Alerting**: Configure alerts for critical metrics
2. **Regular Reviews**: Review dashboards weekly to understand trends
3. **Archive Metrics**: Export important metrics periodically
4. **Test Alerts**: Verify alerts work during low-traffic periods
5. **Document Changes**: Keep track of metric thresholds
6. **Use Service Levels**: Define SLOs (Service Level Objectives) for your API
7. **Monitor Monitoring**: Ensure the monitoring stack itself is healthy

## Further Reading

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Dashboard Guide](https://grafana.com/docs/grafana/latest/dashboards/)
- [FastAPI Monitoring Best Practices](https://fastapi.tiangolo.com/)
- [PostgreSQL Exporter Metrics](https://github.com/prometheuscommunity/postgres_exporter)

## Support

For issues or questions:

1. Check the GitHub Issues section
2. Review Prometheus and Grafana logs
3. Test individual components in isolation
4. Refer to official documentation

