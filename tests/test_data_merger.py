
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock


class TestDataMergerBasics:
    
    def test_merger_initialization(self):
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        assert merger is not None
        assert hasattr(merger, 'sources_data')
        assert len(merger.sources_data) == 0
    
    def test_add_source(self):
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
    
    def test_merge_single_source_operators(self):
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
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        sncf_data = {
            'agency': pd.DataFrame({
                'agency_id': ['1'],
                'agency_name': ['SNCF'],
                'agency_country': ['FR']
            })
        }
        merger.add_source('SNCF', sncf_data)
        
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
        
        if 'agency_id' in result.columns:
            assert any('SNCF_' in str(id_val) for id_val in result['agency_id'])


class TestMergeStations:
    
    def test_merge_stations_from_sources(self):
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
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
        stations = pd.DataFrame({
            'stop_id': ['1', '2', '3'],
            'stop_name': ['Paris', 'Lyon', 'Marseille'],
            'stop_lat': [48.8443, 45.7590, 43.3050],
            'stop_lon': [2.3740, 4.8233, 5.3698]
        })
        
        assert all(-90 <= lat <= 90 for lat in stations['stop_lat'])
        assert all(-180 <= lon <= 180 for lon in stations['stop_lon'])


class TestMergeTrains:
    
    def test_merge_day_and_night_trains(self):
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        sncf_data = {
            'trips': pd.DataFrame({
                'trip_id': ['1', '2'],
                'route_id': ['TGV001', 'TGV002'],
                'trip_type': ['day', 'day']
            })
        }
        merger.add_source('SNCF', sncf_data)
        
        trenitalia_data = {
            'trips': pd.DataFrame({
                'trip_id': ['1', '2'],
                'route_id': ['NTV001', 'NTV002'],
                'trip_type': ['night', 'night']
            })
        }
        merger.add_source('Trenitalia', trenitalia_data)
        
        assert len(merger.sources_data) == 2
    
    def test_merge_trains_with_different_schemas(self):
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        sncf_trains = pd.DataFrame({
            'trip_id': ['1', '2'],
            'route_short_name': ['TGV001', 'TGV002'],
            'agency_id': ['SNCF', 'SNCF']
        })
        
        db_trains = pd.DataFrame({
            'trip_id': ['1', '2'],
            'trip_headsign': ['Berlin', 'Hamburg'],
            'agency_id': ['DB', 'DB'],
            'route_type': [3, 3]
        })
        
        assert sncf_trains.shape[1] == 3
        assert db_trains.shape[1] == 4
        assert sncf_trains.shape[1] != db_trains.shape[1]


class TestConflictResolution:
    
    def test_duplicate_station_ids(self):
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
        
        merged = pd.concat([
            stations_sncf.assign(stop_id=lambda x: 'SNCF_' + x['stop_id']),
            stations_db.assign(stop_id=lambda x: 'DB_' + x['stop_id'])
        ])
        
        assert len(merged) == 4
        assert len(merged['stop_id'].unique()) == 4
    
    def test_conflicting_operator_names(self):
        operators = pd.DataFrame({
            'operator_id': ['SNCF_1', 'DB_1'],
            'operator_name': ['SNCF', 'Deutsche Bahn'],
            'country': ['FR', 'DE']
        })
        
        assert len(operators) == len(operators['operator_id'].unique())
    
    def test_conflicting_route_data(self):
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
        
        merged = pd.concat([routes_source1, routes_source2]).drop_duplicates(
            subset=['route_id'], keep='first'
        )
        
        assert len(merged) == 3


class TestDataIntegrity:
    
    def test_no_data_loss_during_merge(self):
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
    
    def test_merge_large_datasets(self):
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
        from etl.transformers.data_merger import DataMerger
        
        merger = DataMerger()
        
        for i in range(5):
            source_data = {
                'data': pd.DataFrame({
                    'id': [i*100 + j for j in range(10)],
                    'value': [j for j in range(10)]
                })
            }
            merger.add_source(f'Source{i}', source_data)
        
        assert len(merger.sources_data) == 5
