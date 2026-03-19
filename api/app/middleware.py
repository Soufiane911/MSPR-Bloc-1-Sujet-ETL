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
    async def dispatch(self, request: Request, call_next) -> Response:
        method = request.method
        endpoint = request.url.path
        
        http_requests_active.labels(method=method, endpoint=endpoint).inc()
        
        request_size = 0
        if request.headers:
            request_size = len(str(request.headers).encode('utf-8'))
        http_request_size_bytes.labels(method=method, endpoint=endpoint).observe(request_size)
        
        start_time = time.time()
        
        try:
            response = await call_next(request)
            
            duration = time.time() - start_time
            http_request_duration_seconds.labels(method=method, endpoint=endpoint).observe(duration)
            http_requests_total.labels(
                method=method,
                endpoint=endpoint,
                status_code=response.status_code
            ).inc()
            
            response_size = response.body_iterator.__sizeof__() if hasattr(response, 'body_iterator') else 0
            http_response_size_bytes.labels(method=method, endpoint=endpoint).observe(response_size)
            
            if response.status_code >= 400:
                error_type = 'client_error' if response.status_code < 500 else 'server_error'
                api_errors_total.labels(endpoint=endpoint, error_type=error_type).inc()
            
            return response
            
        except Exception as exc:
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
            http_requests_active.labels(method=method, endpoint=endpoint).dec()
