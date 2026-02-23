"""
Prometheus middleware for FastAPI.

This middleware intercepts all HTTP requests and responses to collect metrics
for monitoring application performance.
"""

import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from app.metrics import (
    http_requests_total,
    http_request_duration_seconds,
    http_request_size_bytes,
    http_response_size_bytes,
    http_requests_active,
    api_errors_total,
)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware to collect Prometheus metrics for HTTP requests and responses.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process request and collect metrics.
        
        Args:
            request: The incoming HTTP request
            call_next: The next middleware or route handler
            
        Returns:
            The HTTP response
        """
        # Extract endpoint and method
        method = request.method
        endpoint = request.url.path
        
        # Track active requests
        http_requests_active.labels(method=method, endpoint=endpoint).inc()
        
        # Record request size
        request_size = 0
        if request.headers:
            request_size = len(str(request.headers).encode('utf-8'))
        http_request_size_bytes.labels(method=method, endpoint=endpoint).observe(request_size)
        
        # Start timing
        start_time = time.time()
        
        try:
            # Call the next middleware or route handler
            response = await call_next(request)
            
            # Record response metrics
            duration = time.time() - start_time
            http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)
            http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=response.status_code
            ).inc()
            
            # Record response size
            response_size = response.body_iterator.__sizeof__() if hasattr(response, 'body_iterator') else 0
            http_response_size_bytes.labels(method=method, endpoint=endpoint).observe(response_size)
            
            # Track errors
            if response.status_code >= 400:
                error_type = 'client_error' if response.status_code < 500 else 'server_error'
                api_errors_total.labels(endpoint=endpoint, error_type=error_type).inc()
            
            return response
            
        except Exception as exc:
            # Record error metrics
            duration = time.time() - start_time
            http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)
            http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=500
            ).inc()
            api_errors_total.labels(endpoint=endpoint, error_type='exception').inc()
            raise exc
            
        finally:
            # Decrement active requests
            http_requests_active.labels(method=method, endpoint=endpoint).dec()
