"""
API Endpoint Tests for ObRail Europe FastAPI Application
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from app.main import app

client = TestClient(app)


class TestTrainsEndpoints:
    """Tests for train-related API endpoints."""
    
    @patch('app.services.train_service.TrainService.get_trains')
    def test_get_trains_success(self, mock_get_trains):
        """Test successful retrieval of trains."""
        mock_get_trains.return_value = [
            {
                'train_id': 1,
                'train_number': 'TGV001',
                'operator_name': 'SNCF',
                'train_type': 'day',
                'category': 'TGV',
                'route_name': 'Paris - Lyon'
            },
            {
                'train_id': 2,
                'train_number': 'TGV002',
                'operator_name': 'SNCF',
                'train_type': 'day',
                'category': 'TGV',
                'route_name': 'Paris - Marseille'
            }
        ]
        
        response = client.get("/trains/")
        assert response.status_code == 200
        assert len(response.json()) == 2
        assert response.json()[0]['train_number'] == 'TGV001'
    
    @patch('app.services.train_service.TrainService.get_trains')
    def test_get_trains_with_filters(self, mock_get_trains):
        """Test train retrieval with query filters."""
        mock_get_trains.return_value = [
            {
                'train_id': 1,
                'train_number': 'NTV001',
                'operator_name': 'Trenitalia',
                'train_type': 'night',
                'category': 'Night Train',
                'route_name': 'Rome - Milan'
            }
        ]
        
        response = client.get(
            "/trains/?train_type=night&operator=Trenitalia&country=IT"
        )
        assert response.status_code == 200
        mock_get_trains.assert_called_once()
    
    @patch('app.services.train_service.TrainService.get_trains')
    def test_get_trains_pagination(self, mock_get_trains):
        """Test train pagination with limit and offset."""
        mock_get_trains.return_value = []
        
        response = client.get("/trains/?limit=50&offset=100")
        assert response.status_code == 200
        mock_get_trains.assert_called_once()
    
    @patch('app.services.train_service.TrainService.get_trains')
    def test_get_trains_invalid_limit(self, mock_get_trains):
        """Test that invalid limit parameter is rejected."""
        # Limit should be between 1 and 1000
        response = client.get("/trains/?limit=2000")
        assert response.status_code == 422  # Validation error
    
    @patch('app.services.train_service.TrainService.get_train_by_id')
    def test_get_train_by_id_success(self, mock_get_train):
        """Test successful retrieval of a specific train."""
        from datetime import datetime
        mock_get_train.return_value = {
            'train_id': 1,
            'train_number': 'TGV001',
            'operator_name': 'SNCF',
            'operator_id': 1,
            'train_type': 'day',
            'category': 'TGV',
            'route_name': 'Paris - Lyon',
            'country': 'FR',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        response = client.get("/trains/1")
        assert response.status_code == 200
        assert response.json()['train_id'] == 1
    
    @patch('app.services.train_service.TrainService.get_train_by_id')
    def test_get_train_by_id_not_found(self, mock_get_train):
        """Test retrieval of non-existent train."""
        mock_get_train.return_value = None
        
        response = client.get("/trains/99999")
        assert response.status_code == 404


class TestStationsEndpoints:
    """Tests for station-related API endpoints."""
    
    @patch('app.services.station_service.StationService.get_stations')
    def test_get_stations_success(self, mock_get_stations):
        """Test successful retrieval of stations."""
        mock_get_stations.return_value = [
            {
                'station_id': 1,
                'name': 'Paris Gare de Lyon',
                'city': 'Paris',
                'country': 'FR',
                'latitude': 48.8443,
                'longitude': 2.3740,
                'uic_code': '8700011'
            },
            {
                'station_id': 2,
                'name': 'Lyon Perrache',
                'city': 'Lyon',
                'country': 'FR',
                'latitude': 45.7590,
                'longitude': 4.8233,
                'uic_code': '8700151'
            }
        ]
        
        response = client.get("/stations/")
        assert response.status_code == 200
        assert len(response.json()) == 2
    
    @patch('app.services.station_service.StationService.get_stations')
    def test_get_stations_by_country(self, mock_get_stations):
        """Test station retrieval filtered by country."""
        mock_get_stations.return_value = []
        
        response = client.get("/stations/?country=DE")
        assert response.status_code == 200
        mock_get_stations.assert_called_once()
    
    @patch('app.services.station_service.StationService.get_stations')
    def test_get_stations_by_city(self, mock_get_stations):
        """Test station retrieval filtered by city."""
        mock_get_stations.return_value = []
        
        response = client.get("/stations/?city=Berlin")
        assert response.status_code == 200


class TestSchedulesEndpoints:
    """Tests for schedule-related API endpoints."""
    
    @patch('app.services.schedule_service.ScheduleService.get_schedules')
    def test_get_schedules_success(self, mock_get_schedules):
        """Test successful retrieval of schedules."""
        from datetime import datetime
        mock_get_schedules.return_value = [
            {
                'schedule_id': 1,
                'train_number': 'TGV001',
                'train_type': 'day',
                'origin': 'Paris Gare de Lyon',
                'origin_city': 'Paris',
                'origin_country': 'FR',
                'destination': 'Lyon Perrache',
                'destination_city': 'Lyon',
                'destination_country': 'FR',
                'departure_time': datetime(2026, 2, 23, 8, 0),
                'arrival_time': datetime(2026, 2, 23, 12, 0),
                'duration_min': 240,
                'distance_km': 465.0
            }
        ]
        
        response = client.get("/schedules/")
        assert response.status_code == 200
        assert len(response.json()) == 1


class TestOperatorsEndpoints:
    """Tests for operator-related API endpoints."""
    
    @patch('app.services.operator_service.OperatorService.get_operators')
    def test_get_operators_success(self, mock_get_operators):
        """Test successful retrieval of operators."""
        mock_get_operators.return_value = [
            {
                'operator_id': 1,
                'name': 'SNCF',
                'country': 'FR',
                'website': 'https://www.sncf.com'
            },
            {
                'operator_id': 2,
                'name': 'Deutsche Bahn',
                'country': 'DE',
                'website': 'https://www.deutschebahn.com'
            }
        ]
        
        response = client.get("/operators/")
        assert response.status_code == 200
        assert len(response.json()) == 2


class TestErrorHandling:
    """Tests for error handling in API endpoints."""
    
    def test_invalid_endpoint(self):
        """Test response for non-existent endpoint."""
        response = client.get("/nonexistent/")
        assert response.status_code == 404
    
    def test_server_error_handling(self):
        """Test handling of server errors gracefully."""
        # Test that invalid requests are handled properly
        # Using a malformed query that should trigger validation error
        response = client.get("/trains/?limit=invalid_value")
        # Should return validation error (422)
        assert response.status_code in [422, 400]
    
    def test_invalid_query_parameter_type(self):
        """Test handling of invalid query parameter types."""
        response = client.get("/trains/?limit=invalid")
        assert response.status_code == 422  # Validation error


class TestAPIDocumentation:
    """Tests for API documentation endpoints."""
    
    def test_swagger_docs_available(self):
        """Test that Swagger documentation is available."""
        response = client.get("/docs")
        assert response.status_code == 200
    
    def test_redoc_docs_available(self):
        """Test that ReDoc documentation is available."""
        response = client.get("/redoc")
        assert response.status_code == 200
    
    def test_openapi_schema_available(self):
        """Test that OpenAPI schema is available."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        assert 'openapi' in response.json()
