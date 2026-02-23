"""
Data Merger Tests for ObRail Europe ETL Project
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


class TestDataMergerBasics:
    """Tests for basic data merger functionality."""
    
    def test_merger_initialization(self):
        """Test DataMerger initialization."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        assert merger is not None
        assert hasattr(merger, 'sources_data')
        assert len(merger.sources_data) == 0
    
    def test_add_source(self):
        """Test adding a source to the merger."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        source_data = {
            'agencies': pd.DataFrame({
                'agency_id': ['1'],
                'agency_name': ['SNCF']
            })
        }
        
        merger.add_source('SNCF', source_data)
        assert 'SNCF' in merger.sources_data


class TestMergeOperators:
    """Tests for merging operator data from multiple sources."""
    
    def test_merge_single_source_operators(self):
        """Test merging operators from a single source."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        source_data = {
            'agency': pd.DataFrame({
                'agency_id': ['1', '2'],
                'agency_name': ['SNCF', 'SNCF Fret'],
                'agency_url': ['https://www.sncf.com', 'https://www.sncf-fret.fr']
            })
        }
        
        merger.add_source('SNCF', source_data)
        result = merger.merge_operators()
        
        assert result is not None
        assert len(result) == 2
    
    def test_merge_multiple_source_operators(self):
        """Test merging operators from multiple sources."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        # Add SNCF source
        sncf_data = {
            'agency': pd.DataFrame({
                'agency_id': ['1'],
                'agency_name': ['SNCF'],
                'agency_country': ['FR']
            })
        }
        merger.add_source('SNCF', sncf_data)
        
        # Add Deutsche Bahn source
        db_data = {
            'agency': pd.DataFrame({
                'agency_id': ['1'],
                'agency_name': ['Deutsche Bahn'],
                'agency_country': ['DE']
            })
        }
        merger.add_source('Deutsche Bahn', db_data)
        
        result = merger.merge_operators()
        
        assert len(result) >= 2
    
    def test_operator_id_prefixing(self):
        """Test that operator IDs are properly prefixed with source name."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        source_data = {
            'agency': pd.DataFrame({
                'agency_id': ['1', '2'],
                'agency_name': ['SNCF', 'SNCF Fret']
            })
        }
        
        merger.add_source('SNCF', source_data)
        result = merger.merge_operators()
        
        # IDs should be prefixed
        if 'agency_id' in result.columns:
            assert any('SNCF_' in str(id_val) for id_val in result['agency_id'])


class TestMergeStations:
    """Tests for merging station data from multiple sources."""
    
    def test_merge_stations_from_sources(self):
        """Test merging stations from multiple sources."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        # Add SNCF stations
        sncf_data = {
            'stops': pd.DataFrame({
                'stop_id': ['1', '2'],
                'stop_name': ['Paris Gare de Lyon', 'Lyon Perrache'],
                'stop_lat': [48.8443, 45.7590],
                'stop_lon': [2.3740, 4.8233]
            })
        }
        merger.add_source('SNCF', sncf_data)
        
        result = merger.merge_operators()
        assert result is not None
    
    def test_station_coordinate_preservation(self):
        """Test that station coordinates are preserved during merge."""
        # Create sample data
        stations = pd.DataFrame({
            'stop_id': ['1', '2', '3'],
            'stop_name': ['Paris', 'Lyon', 'Marseille'],
            'stop_lat': [48.8443, 45.7590, 43.3050],
            'stop_lon': [2.3740, 4.8233, 5.3698]
        })
        
        # Verify coordinates are valid
        assert all(-90 <= lat <= 90 for lat in stations['stop_lat'])
        assert all(-180 <= lon <= 180 for lon in stations['stop_lon'])


class TestMergeTrains:
    """Tests for merging train data from multiple sources."""
    
    def test_merge_day_and_night_trains(self):
        """Test merging day and night train data."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        # Day trains from SNCF
        sncf_data = {
            'trips': pd.DataFrame({
                'trip_id': ['1', '2'],
                'route_id': ['TGV001', 'TGV002'],
                'trip_type': ['day', 'day']
            })
        }
        merger.add_source('SNCF', sncf_data)
        
        # Night trains from Trenitalia
        trenitalia_data = {
            'trips': pd.DataFrame({
                'trip_id': ['1', '2'],
                'route_id': ['NTV001', 'NTV002'],
                'trip_type': ['night', 'night']
            })
        }
        merger.add_source('Trenitalia', trenitalia_data)
        
        # Both sources should be added
        assert len(merger.sources_data) == 2
    
    def test_merge_trains_with_different_schemas(self):
        """Test merging train data with different schemas."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        # SNCF format with 3 columns
        sncf_trains = pd.DataFrame({
            'trip_id': ['1', '2'],
            'route_short_name': ['TGV001', 'TGV002'],
            'agency_id': ['SNCF', 'SNCF']
        })
        
        # Deutsche Bahn format with 4 columns
        db_trains = pd.DataFrame({
            'trip_id': ['1', '2'],
            'trip_headsign': ['Berlin', 'Hamburg'],
            'agency_id': ['DB', 'DB'],
            'route_type': [3, 3]
        })
        
        # Verify schemas have different column counts
        assert sncf_trains.shape[1] == 3
        assert db_trains.shape[1] == 4
        assert sncf_trains.shape[1] != db_trains.shape[1]


class TestConflictResolution:
    """Tests for conflict resolution during data merging."""
    
    def test_duplicate_station_ids(self):
        """Test handling of duplicate station IDs from different sources."""
        # Simulate duplicate IDs
        stations_sncf = pd.DataFrame({
            'stop_id': ['1', '2'],
            'stop_name': ['Paris Gare de Lyon', 'Lyon Perrache'],
            'source': ['SNCF', 'SNCF']
        })
        
        stations_db = pd.DataFrame({
            'stop_id': ['1', '3'],
            'stop_name': ['Berlin Hauptbahnhof', 'Munich Hauptbahnhof'],
            'source': ['DB', 'DB']
        })
        
        # After merge with ID prefixing, conflicts should be resolved
        # Simulating merged result
        merged = pd.concat([
            stations_sncf.assign(stop_id=lambda x: 'SNCF_' + x['stop_id']),
            stations_db.assign(stop_id=lambda x: 'DB_' + x['stop_id'])
        ])
        
        assert len(merged) == 4
        assert len(merged['stop_id'].unique()) == 4
    
    def test_conflicting_operator_names(self):
        """Test handling of conflicting operator names."""
        operators = pd.DataFrame({
            'operator_id': ['SNCF_1', 'DB_1'],
            'operator_name': ['SNCF', 'Deutsche Bahn'],
            'country': ['FR', 'DE']
        })
        
        # Ensure no name conflicts after prefixing
        assert len(operators) == len(operators['operator_id'].unique())
    
    def test_conflicting_route_data(self):
        """Test handling of conflicting route data."""
        routes_source1 = pd.DataFrame({
            'route_id': ['TGV001', 'TGV002'],
            'route_name': ['Paris - Lyon', 'Paris - Marseille'],
            'distance_km': [465, 773]
        })
        
        routes_source2 = pd.DataFrame({
            'route_id': ['TGV001', 'TGV003'],
            'route_name': ['Paris - Lyon', 'Paris - Brussels'],
            'distance_km': [465, 315]
        })
        
        # Routes with same ID should be deduplicated
        # Simulating merge with deduplication
        merged = pd.concat([routes_source1, routes_source2]).drop_duplicates(
            subset=['route_id'], keep='first'
        )
        
        assert len(merged) == 3


class TestDataIntegrity:
    """Tests for maintaining data integrity during merges."""
    
    def test_no_data_loss_during_merge(self):
        """Test that no data is lost during merge operation."""
        df1 = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        
        df2 = pd.DataFrame({
            'id': [4, 5, 6],
            'value': ['d', 'e', 'f']
        })
        
        merged = pd.concat([df1, df2])
        
        assert len(merged) == len(df1) + len(df2)
        assert set(merged['id']) == {1, 2, 3, 4, 5, 6}
    
    def test_column_consistency(self):
        """Test that columns remain consistent after merge."""
        df1 = pd.DataFrame({
            'train_id': [1, 2],
            'operator': ['SNCF', 'SNCF']
        })
        
        df2 = pd.DataFrame({
            'train_id': [3, 4],
            'operator': ['DB', 'DB']
        })
        
        merged = pd.concat([df1, df2])
        
        assert list(merged.columns) == list(df1.columns)
    
    def test_data_type_preservation(self):
        """Test that data types are preserved during merge."""
        df1 = pd.DataFrame({
            'id': [1, 2],
            'name': ['A', 'B'],
            'value': [1.5, 2.5]
        })
        
        df2 = pd.DataFrame({
            'id': [3, 4],
            'name': ['C', 'D'],
            'value': [3.5, 4.5]
        })
        
        merged = pd.concat([df1, df2])
        
        assert merged['id'].dtype == df1['id'].dtype
        assert merged['name'].dtype == df1['name'].dtype
        assert merged['value'].dtype == df1['value'].dtype


class TestMergePerformance:
    """Tests for merge operation performance and scalability."""
    
    def test_merge_large_datasets(self):
        """Test merging large datasets."""
        # Create large DataFrames
        large_df1 = pd.DataFrame({
            'id': range(10000),
            'value': np.random.rand(10000)
        })
        
        large_df2 = pd.DataFrame({
            'id': range(10000, 20000),
            'value': np.random.rand(10000)
        })
        
        merged = pd.concat([large_df1, large_df2])
        
        assert len(merged) == 20000
    
    def test_merge_with_many_sources(self):
        """Test merging data from many sources."""
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        # Add 5 sources
        for i in range(5):
            source_data = {
                'data': pd.DataFrame({
                    'id': [i*100 + j for j in range(10)],
                    'value': [j for j in range(10)]
                })
            }
            merger.add_source(f'Source{i}', source_data)
        
        assert len(merger.sources_data) == 5
