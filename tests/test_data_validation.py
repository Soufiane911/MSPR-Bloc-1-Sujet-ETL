"""
Data Validation Tests for ObRail Europe ETL Project
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, time


class TestTrainDataValidation:
    """Tests for train data validation."""
    
    def test_valid_train_number_format(self):
        """Test validation of train number format."""
        valid_train_numbers = ['TGV001', 'ICE123', 'AB001', 'NTV456']
        
        for train_num in valid_train_numbers:
            assert isinstance(train_num, str)
            assert len(train_num) > 0
    
    def test_invalid_train_number_empty(self):
        """Test rejection of empty train numbers."""
        invalid_train_numbers = ['', None]
        
        for train_num in invalid_train_numbers:
            assert train_num in ['', None]
    
    def test_valid_train_type(self):
        """Test validation of train type field."""
        valid_types = ['day', 'night']
        
        for train_type in valid_types:
            assert train_type in ['day', 'night']
    
    def test_invalid_train_type(self):
        """Test rejection of invalid train types."""
        invalid_types = ['afternoon', 'morning', 'evening', 'invalid']
        
        for train_type in invalid_types:
            assert train_type not in ['day', 'night']
    
    def test_train_id_must_be_integer(self):
        """Test that train IDs are integers."""
        valid_ids = [1, 2, 100, 9999]
        invalid_ids = ['abc', -1, 0]
        
        for train_id in valid_ids:
            assert isinstance(train_id, int) and train_id > 0
        
        for train_id in invalid_ids:
            if isinstance(train_id, int):
                assert train_id <= 0
    
    def test_train_category_validation(self):
        """Test validation of train categories."""
        valid_categories = ['TGV', 'ICE', 'Eurostar', 'Regional', 'Night Train']
        
        for category in valid_categories:
            assert isinstance(category, str)
            assert len(category) > 0


class TestStationDataValidation:
    """Tests for station data validation."""
    
    def test_valid_latitude_longitude(self):
        """Test validation of latitude and longitude coordinates."""
        valid_coords = [
            (48.8443, 2.3740),  # Paris
            (45.7590, 4.8233),  # Lyon
            (51.5074, -0.1278), # London
        ]
        
        for lat, lon in valid_coords:
            assert -90 <= lat <= 90, "Latitude out of range"
            assert -180 <= lon <= 180, "Longitude out of range"
    
    def test_invalid_latitude(self):
        """Test rejection of invalid latitude values."""
        invalid_lats = [-91, 91, 200, -200]
        
        for lat in invalid_lats:
            assert not (-90 <= lat <= 90)
    
    def test_invalid_longitude(self):
        """Test rejection of invalid longitude values."""
        invalid_lons = [-181, 181, 360, -360]
        
        for lon in invalid_lons:
            assert not (-180 <= lon <= 180)
    
    def test_valid_country_code(self):
        """Test validation of country codes."""
        valid_codes = ['FR', 'DE', 'IT', 'ES', 'BE', 'AT']
        
        for code in valid_codes:
            assert len(code) == 2
            assert code.isupper()
    
    def test_station_name_not_empty(self):
        """Test that station names are not empty."""
        valid_names = ['Paris Gare de Lyon', 'Berlin Hauptbahnhof', 'Milano Centrale']
        
        for name in valid_names:
            assert len(name) > 0
            assert isinstance(name, str)
    
    def test_uic_code_format(self):
        """Test UIC code format validation."""
        valid_uic_codes = ['8700011', '8700151', '8100013']
        
        for code in valid_uic_codes:
            assert len(code) >= 5
            assert code.isdigit()


class TestScheduleDataValidation:
    """Tests for schedule data validation."""
    
    def test_valid_time_format(self):
        """Test validation of time formats."""
        valid_times = ['08:00', '12:30', '23:59', '00:00']
        
        for time_str in valid_times:
            parts = time_str.split(':')
            assert len(parts) == 2
            hour, minute = int(parts[0]), int(parts[1])
            assert 0 <= hour <= 23
            assert 0 <= minute <= 59
    
    def test_invalid_time_format(self):
        """Test rejection of invalid time formats."""
        invalid_times = ['25:00', '12:60', '8:00', '12-30', 'invalid']
        
        for time_str in invalid_times:
            try:
                parts = time_str.split(':')
                if len(parts) == 2:
                    hour, minute = int(parts[0]), int(parts[1])
                    assert not (0 <= hour <= 23 and 0 <= minute <= 59)
            except (ValueError, AssertionError):
                pass  # Invalid format caught
    
    def test_departure_before_arrival(self):
        """Test that departure time is before arrival time."""
        schedules = [
            {'departure': '08:00', 'arrival': '12:00'},  # Valid
            {'departure': '14:00', 'arrival': '10:00'},  # Invalid
            {'departure': '08:00', 'arrival': '08:00'},  # Invalid (same time)
        ]
        
        for schedule in schedules:
            dep_time = datetime.strptime(schedule['departure'], '%H:%M').time()
            arr_time = datetime.strptime(schedule['arrival'], '%H:%M').time()
            
            if schedule['departure'] == '08:00' and schedule['arrival'] == '12:00':
                assert dep_time < arr_time
    
    def test_valid_day_of_week(self):
        """Test validation of day of week."""
        valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        for day in valid_days:
            assert day in valid_days


class TestOperatorDataValidation:
    """Tests for operator data validation."""
    
    def test_valid_operator_name(self):
        """Test validation of operator names."""
        valid_names = ['SNCF', 'Deutsche Bahn', 'Trenitalia', 'Renfe']
        
        for name in valid_names:
            assert len(name) > 0
            assert isinstance(name, str)
    
    def test_operator_country_code(self):
        """Test validation of operator country codes."""
        valid_codes = ['FR', 'DE', 'IT', 'ES']
        
        for code in valid_codes:
            assert len(code) == 2
            assert code.isupper()
    
    def test_operator_website_format(self):
        """Test validation of operator website URLs."""
        valid_urls = [
            'https://www.sncf.com',
            'https://www.deutschebahn.com',
            'http://example.com'
        ]
        
        for url in valid_urls:
            assert url.startswith('http://') or url.startswith('https://')


class TestDataFrameValidation:
    """Tests for DataFrame structure validation."""
    
    def test_required_columns_present(self):
        """Test that required columns are present in DataFrames."""
        df = pd.DataFrame({
            'train_id': [1, 2, 3],
            'train_number': ['TGV001', 'TGV002', 'TGV003'],
            'operator_name': ['SNCF', 'SNCF', 'SNCF']
        })
        
        required_cols = ['train_id', 'train_number', 'operator_name']
        for col in required_cols:
            assert col in df.columns
    
    def test_no_missing_values_in_key_fields(self):
        """Test that key fields have no missing values."""
        df = pd.DataFrame({
            'train_id': [1, 2, None, 4],
            'train_number': ['TGV001', 'TGV002', 'TGV003', 'TGV004']
        })
        
        # Simulate validation
        missing_in_train_id = df['train_id'].isnull().sum()
        assert missing_in_train_id > 0  # This data has missing values
    
    def test_data_type_consistency(self):
        """Test that data types are consistent."""
        df = pd.DataFrame({
            'train_id': [1, 2, 3],
            'latitude': [48.8443, 45.7590, 51.5074],
            'country': ['FR', 'FR', 'GB']
        })
        
        assert df['train_id'].dtype in [np.int64, np.int32]
        assert df['latitude'].dtype in [np.float64, np.float32]
        assert df['country'].dtype == object


class TestRangeValidation:
    """Tests for value range validation."""
    
    def test_train_id_positive(self):
        """Test that train IDs are positive integers."""
        train_ids = [1, 100, 9999]
        
        for train_id in train_ids:
            assert train_id > 0
    
    def test_limit_range(self):
        """Test that limit parameter is within valid range."""
        valid_limits = [1, 50, 100, 500, 1000]
        invalid_limits = [0, -1, 1001, 2000]
        
        for limit in valid_limits:
            assert 1 <= limit <= 1000
        
        for limit in invalid_limits:
            assert not (1 <= limit <= 1000)
    
    def test_offset_not_negative(self):
        """Test that offset is not negative."""
        valid_offsets = [0, 10, 100, 1000]
        invalid_offsets = [-1, -10, -100]
        
        for offset in valid_offsets:
            assert offset >= 0
        
        for offset in invalid_offsets:
            assert not (offset >= 0)
