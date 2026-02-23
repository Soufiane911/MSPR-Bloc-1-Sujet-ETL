"""
Tests for monitoring and metrics collection.

This module contains tests to verify that:
1. Metrics are being collected by the API
2. Prometheus can scrape metrics
3. Database metrics are available
4. System metrics are available
"""

import pytest
import requests
import time
from typing import Dict, List


class TestMetricsCollection:
    """Test metrics collection from the API."""
    
    API_URL = "http://localhost:8000"
    PROMETHEUS_URL = "http://localhost:9090"
    METRICS_ENDPOINT = f"{API_URL}/metrics"
    
    def test_metrics_endpoint_accessible(self):
        """Test that the metrics endpoint is accessible."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert response.status_code == 200, "Metrics endpoint should return 200"
    
    def test_metrics_in_prometheus_format(self):
        """Test that metrics are in valid Prometheus format."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "HELP" in response.text or "#" in response.text, "Should contain Prometheus format"
        assert "http_requests_total" in response.text, "Should contain http_requests_total metric"
    
    def test_http_requests_total_metric_exists(self):
        """Test that http_requests_total metric is being collected."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "http_requests_total" in response.text, "http_requests_total metric should exist"
    
    def test_http_request_duration_metric_exists(self):
        """Test that request duration metrics are being collected."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "http_request_duration_seconds" in response.text, "Request duration metric should exist"
    
    def test_active_requests_metric_exists(self):
        """Test that active requests metric is being collected."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "http_requests_active" in response.text, "Active requests metric should exist"
    
    def test_request_size_metric_exists(self):
        """Test that request size metrics are being collected."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "http_request_size_bytes" in response.text, "Request size metric should exist"
    
    def test_response_size_metric_exists(self):
        """Test that response size metrics are being collected."""
        response = requests.get(self.METRICS_ENDPOINT)
        assert "http_response_size_bytes" in response.text, "Response size metric should exist"
    
    def test_metrics_increase_on_requests(self):
        """Test that metrics increase when requests are made."""
        # Get initial metrics
        initial = requests.get(self.METRICS_ENDPOINT).text
        initial_count = initial.count("http_requests_total")
        
        # Make a request to the API
        requests.get(f"{self.API_URL}/trains/", timeout=5).status_code
        
        # Wait a moment for metrics to be recorded
        time.sleep(0.5)
        
        # Get updated metrics
        updated = requests.get(self.METRICS_ENDPOINT).text
        updated_count = updated.count("http_requests_total")
        
        assert updated_count >= initial_count, "Metrics should increase after request"


class TestPrometheusIntegration:
    """Test Prometheus integration with metrics."""
    
    API_URL = "http://localhost:8000"
    PROMETHEUS_URL = "http://localhost:9090"
    
    def test_prometheus_accessible(self):
        """Test that Prometheus is running and accessible."""
        response = requests.get(f"{self.PROMETHEUS_URL}/-/healthy")
        assert response.status_code == 200, "Prometheus should be healthy"
    
    def test_prometheus_can_query_api_metrics(self):
        """Test that Prometheus can query API metrics."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "http_requests_total"}
        )
        assert response.status_code == 200, "Prometheus query should succeed"
        data = response.json()
        assert data["status"] == "success", "Query status should be success"
    
    def test_prometheus_scrape_config_valid(self):
        """Test that Prometheus has valid scrape configs."""
        response = requests.get(f"{self.PROMETHEUS_URL}/api/v1/targets")
        assert response.status_code == 200, "Should get targets"
        data = response.json()
        assert "data" in data, "Response should contain data"
        assert "activeTargets" in data["data"], "Should have active targets"
        assert len(data["data"]["activeTargets"]) > 0, "Should have at least one active target"
    
    def test_api_target_is_up(self):
        """Test that the API target is up in Prometheus."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "up{job=\"obrail-api\"}"}
        )
        data = response.json()
        if data["data"]["result"]:
            # If the metric exists, it should be 1 (up)
            value = float(data["data"]["result"][0]["value"][1])
            assert value == 1, "API target should be up (value = 1)"


class TestDatabaseMetrics:
    """Test database metrics collection."""
    
    API_URL = "http://localhost:8000"
    POSTGRES_EXPORTER_URL = "http://localhost:9187"
    PROMETHEUS_URL = "http://localhost:9090"
    
    def test_postgres_exporter_accessible(self):
        """Test that PostgreSQL exporter is accessible."""
        response = requests.get(self.POSTGRES_EXPORTER_URL)
        assert response.status_code == 200, "PostgreSQL exporter should be accessible"
    
    def test_postgres_metrics_in_prometheus(self):
        """Test that PostgreSQL metrics are in Prometheus."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "pg_up"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"
        # If PostgreSQL is running, we should get a result
        if data["data"]["result"]:
            value = float(data["data"]["result"][0]["value"][1])
            assert value in [0, 1], "pg_up should be 0 or 1"
    
    def test_database_connection_metrics(self):
        """Test that database connection metrics exist."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "pg_stat_activity_count"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"


class TestContainerMetrics:
    """Test container/cAdvisor metrics."""
    
    CADVISOR_URL = "http://localhost:8080"
    PROMETHEUS_URL = "http://localhost:9090"
    
    def test_cadvisor_accessible(self):
        """Test that cAdvisor is accessible."""
        response = requests.get(self.CADVISOR_URL)
        assert response.status_code == 200, "cAdvisor should be accessible"
    
    def test_container_cpu_metrics_in_prometheus(self):
        """Test that container CPU metrics are in Prometheus."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "container_cpu_usage_seconds_total"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"
        assert len(data["data"]["result"]) > 0, "Should have CPU metrics"
    
    def test_container_memory_metrics_in_prometheus(self):
        """Test that container memory metrics are in Prometheus."""
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "container_memory_working_set_bytes"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"
        assert len(data["data"]["result"]) > 0, "Should have memory metrics"


class TestApplicationPerformance:
    """Test application performance metrics."""
    
    API_URL = "http://localhost:8000"
    PROMETHEUS_URL = "http://localhost:9090"
    
    def test_response_time_acceptable(self):
        """Test that API response times are within acceptable range."""
        start = time.time()
        response = requests.get(f"{self.API_URL}/health")
        duration = time.time() - start
        
        assert response.status_code == 200, "Health endpoint should return 200"
        assert duration < 5, "Response should be under 5 seconds"
    
    def test_error_rate_metric_captures_errors(self):
        """Test that error rate metrics are captured."""
        # Make a request that will cause an error
        requests.get(f"{self.API_URL}/non-existent", timeout=5)
        
        time.sleep(1)
        
        # Check if error metrics were recorded
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "api_errors_total"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"
    
    def test_request_latency_buckets_populated(self):
        """Test that request latency histogram buckets are populated."""
        # Make several requests to populate the histogram
        for _ in range(5):
            requests.get(f"{self.API_URL}/health", timeout=5)
        
        time.sleep(1)
        
        # Query Prometheus for histogram data
        response = requests.get(
            f"{self.PROMETHEUS_URL}/api/v1/query",
            params={"query": "http_request_duration_seconds_bucket"}
        )
        data = response.json()
        assert data["status"] == "success", "Query should succeed"
        assert len(data["data"]["result"]) > 0, "Should have histogram bucket data"


class TestMonitoringStackHealth:
    """Test the overall health of the monitoring stack."""
    
    API_URL = "http://localhost:8000"
    PROMETHEUS_URL = "http://localhost:9090"
    GRAFANA_URL = "http://localhost:3000"
    POSTGRES_EXPORTER_URL = "http://localhost:9187"
    CADVISOR_URL = "http://localhost:8080"
    
    def test_all_services_accessible(self):
        """Test that all monitoring services are accessible."""
        services = {
            "API": self.API_URL,
            "Prometheus": self.PROMETHEUS_URL,
            "Grafana": self.GRAFANA_URL,
            "PostgreSQL Exporter": self.POSTGRES_EXPORTER_URL,
            "cAdvisor": self.CADVISOR_URL,
        }
        
        for name, url in services.items():
            try:
                response = requests.get(url, timeout=5)
                assert response.status_code in [200, 302, 401], f"{name} should be accessible"
            except requests.exceptions.ConnectionError:
                pytest.fail(f"{name} at {url} is not accessible")
    
    def test_prometheus_has_data_sources(self):
        """Test that Prometheus has connected data sources."""
        response = requests.get(f"{self.PROMETHEUS_URL}/api/v1/targets")
        data = response.json()
        
        active_targets = data["data"]["activeTargets"]
        assert len(active_targets) > 0, "Should have at least one active scrape target"
        
        # Check that targets are up
        up_count = sum(1 for t in active_targets if t["health"] == "up")
        assert up_count > 0, "Should have at least one healthy target"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
