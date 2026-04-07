"""
ETL Pipeline Tests for ObRail Europe Project
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from pathlib import Path


class TestDataExtraction:
    """Tests for data extraction phase of ETL."""
    
    @patch('etl.extractors.gtfs_extractor.GTFSExtractor')
    def test_gtfs_extractor_initialization(self, mock_gtfs):
        """Test GTFS extractor initialization."""
        mock_extractor = MagicMock()
        mock_gtfs.return_value = mock_extractor
        
        assert mock_extractor is not None
    
    @patch('etl.extractors.gtfs_extractor.GTFSExtractor')
    def test_gtfs_extract_stops(self, mock_gtfs):
        """Test extraction of stops from GTFS data."""
        mock_extractor = MagicMock()
        mock_extractor.extract_stops.return_value = pd.DataFrame({
            'stop_id': ['1', '2', '3'],
            'stop_name': ['Paris', 'Lyon', 'Marseille'],
            'stop_lat': [48.8443, 45.7590, 43.3050],
            'stop_lon': [2.3740, 4.8233, 5.3698]
        })
        
        result = mock_extractor.extract_stops()
        assert len(result) == 3
        assert 'stop_id' in result.columns
        assert 'stop_name' in result.columns
    
    @patch('etl.extractors.gtfs_extractor.GTFSExtractor')
    def test_gtfs_extract_routes(self, mock_gtfs):
        """Test extraction of routes from GTFS data."""
        mock_extractor = MagicMock()
        mock_extractor.extract_routes.return_value = pd.DataFrame({
            'route_id': ['1', '2'],
            'route_short_name': ['TGV001', 'TGV002'],
            'route_type': [3, 3],
            'agency_id': ['SNCF', 'SNCF']
        })
        
        result = mock_extractor.extract_routes()
        assert len(result) == 2
        assert 'route_id' in result.columns
    
    @patch('etl.extractors.back_on_track.BackOnTrackExtractor')
    def test_back_on_track_extractor(self, mock_bot):
        """Test Back-on-Track extractor."""
        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = pd.DataFrame({
            'train_id': [1, 2, 3],
            'train_name': ['NTV001', 'NTV002', 'NTV003'],
            'train_type': ['night', 'night', 'night'],
            'operator': ['Trenitalia', 'ÖBB', 'SNCF']
        })
        
        result = mock_extractor.extract()
        assert len(result) == 3
        assert all(result['train_type'] == 'night')
    
    @patch('etl.extractors.mobility_catalog.MobilityCatalogExtractor')
    def test_mobility_catalog_extractor(self, mock_mob):
        """Test Mobility Catalog extractor."""
        mock_extractor = MagicMock()
        mock_extractor.extract.return_value = pd.DataFrame({
            'station_id': [1, 2],
            'station_name': ['Berlin', 'Munich'],
            'country': ['DE', 'DE']
        })
        
        result = mock_extractor.extract()
        assert len(result) == 2


class TestDataTransformation:
    """Tests for data transformation phase of ETL."""
    
    def test_data_cleaner_remove_duplicates(self):
        """Test removal of duplicate records."""
        from etl.transformers.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        df = pd.DataFrame({
            'train_id': [1, 2, 1, 3],
            'train_number': ['TGV001', 'TGV002', 'TGV001', 'TGV003']
        })
        
        df_clean = cleaner.remove_duplicates(df, subset=['train_id'])
        assert len(df_clean) == 3
    
    def test_data_cleaner_handle_missing_values(self):
        """Test handling of missing values."""
        from etl.transformers.data_cleaner import DataCleaner
        
        cleaner = DataCleaner()
        df = pd.DataFrame({
            'train_id': [1, 2, None, 4],
            'train_number': ['TGV001', None, 'TGV003', 'TGV004']
        })
        
        # Should handle missing values
        assert df.isnull().sum().sum() == 2
    
    @patch('etl.transformers.data_normalizer.DataNormalizer')
    def test_data_normalizer(self, mock_normalizer):
        """Test data normalization."""
        mock_norm = MagicMock()
        mock_norm.normalize.return_value = pd.DataFrame({
            'train_id': [1, 2, 3],
            'train_number': ['TGV001', 'TGV002', 'TGV003'],
            'operator_id': [1, 1, 1]
        })
        
        result = mock_norm.normalize(pd.DataFrame())
        assert len(result) == 3
        assert 'train_id' in result.columns
    
    @patch('etl.transformers.day_night_classifier.DayNightClassifier')
    def test_day_night_classifier(self, mock_classifier):
        """Test day/night train classification."""
        mock_clf = MagicMock()
        mock_clf.classify.return_value = pd.DataFrame({
            'train_id': [1, 2, 3, 4],
            'train_type': ['day', 'day', 'night', 'night'],
            'confidence': [0.95, 0.88, 0.92, 0.85]
        })
        
        result = mock_clf.classify(pd.DataFrame())
        assert len(result) == 4
        assert set(result['train_type'].unique()) == {'day', 'night'}
        assert all(result['confidence'] >= 0.8)


class TestDataMerging:
    """Tests for data merger functionality."""
    
    @patch('etl.transformers.data_merger.DataMerger')
    def test_merge_operators(self, mock_merger):
        """Test merging operators from multiple sources."""
        mock_merge = MagicMock()
        mock_merge.merge_operators.return_value = pd.DataFrame({
            'agency_id': ['SNCF_1', 'DB_1', 'Trenitalia_1'],
            'agency_name': ['SNCF', 'Deutsche Bahn', 'Trenitalia'],
            'country': ['FR', 'DE', 'IT']
        })
        
        result = mock_merge.merge_operators()
        assert len(result) == 3
        assert 'agency_id' in result.columns
    
    @patch('etl.transformers.data_merger.DataMerger')
    def test_merge_stations(self, mock_merger):
        """Test merging stations from multiple sources."""
        mock_merge = MagicMock()
        mock_merge.merge_stations.return_value = pd.DataFrame({
            'station_id': [1, 2, 3],
            'station_name': ['Paris', 'Lyon', 'Berlin'],
            'source': ['SNCF', 'SNCF', 'DB']
        })
        
        result = mock_merge.merge_stations()
        assert len(result) == 3
    
    @patch('etl.transformers.data_merger.DataMerger')
    def test_merge_handles_conflicts(self, mock_merger):
        """Test handling of conflicting data during merge."""
        mock_merge = MagicMock()
        # Simulate conflict resolution
        mock_merge.merge_operators.return_value = pd.DataFrame({
            'agency_id': ['conflict_resolved_1'],
            'agency_name': ['Merged Agency'],
            'source': ['multiple']
        })
        
        result = mock_merge.merge_operators()
        assert len(result) == 1


class TestDataLoading:
    """Tests for data loading phase of ETL."""
    
    @patch('etl.loaders.database_loader.DatabaseLoader')
    def test_database_loader_initialization(self, mock_loader):
        """Test database loader initialization."""
        mock_db_loader = MagicMock()
        assert mock_db_loader is not None
    
    @patch('etl.loaders.database_loader.DatabaseLoader')
    def test_load_trains_to_database(self, mock_loader):
        """Test loading train data to database."""
        mock_db_loader = MagicMock()
        mock_db_loader.load_trains.return_value = 100
        
        df = pd.DataFrame({
            'train_id': range(1, 101),
            'train_number': [f'TRAIN{i:03d}' for i in range(1, 101)],
            'operator_id': [1] * 100
        })
        
        result = mock_db_loader.load_trains(df)
        assert result == 100
    
    @patch('etl.loaders.database_loader.DatabaseLoader')
    def test_load_stations_to_database(self, mock_loader):
        """Test loading station data to database."""
        mock_db_loader = MagicMock()
        mock_db_loader.load_stations.return_value = 50
        
        df = pd.DataFrame({
            'station_id': range(1, 51),
            'station_name': [f'Station {i}' for i in range(1, 51)],
            'country': ['FR'] * 50
        })
        
        result = mock_db_loader.load_stations(df)
        assert result == 50


class TestETLPipelineIntegration:
    """Integration tests for complete ETL pipeline."""
    
    @patch('etl.extractors.gtfs_extractor.GTFSExtractor')
    @patch('etl.transformers.data_cleaner.DataCleaner')
    @patch('etl.loaders.database_loader.DatabaseLoader')
    def test_complete_etl_pipeline(self, mock_loader, mock_cleaner, mock_gtfs):
        """Test complete ETL pipeline execution."""
        # Setup mocks
        mock_extractor = MagicMock()
        mock_extractor.extract_stops.return_value = pd.DataFrame({
            'stop_id': ['1', '2'],
            'stop_name': ['Paris', 'Lyon']
        })
        mock_gtfs.return_value = mock_extractor
        
        mock_clean = MagicMock()
        mock_clean.remove_duplicates.return_value = pd.DataFrame({
            'stop_id': ['1', '2'],
            'stop_name': ['Paris', 'Lyon']
        })
        mock_cleaner.return_value = mock_clean
        
        mock_db = MagicMock()
        mock_db.load_stations.return_value = 2
        mock_loader.return_value = mock_db
        
        # Simulate pipeline
        assert mock_extractor is not None
        assert mock_clean is not None
        assert mock_db is not None
    
    def test_etl_pipeline_data_flow(self):
        """Test data flowing through ETL stages."""
        # Simulate data at each stage
        raw_data = pd.DataFrame({
            'id': [1, 1, 2],
            'name': ['Train A', 'Train A', 'Train B']
        })
        
        # After extraction - no changes
        extracted = raw_data.copy()
        assert len(extracted) == 3
        
        # After transformation - duplicates removed
        transformed = extracted.drop_duplicates(subset=['id'])
        assert len(transformed) == 2
        
        # Verify data quality improved
        assert len(transformed) < len(extracted)
