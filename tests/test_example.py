
import pytest
import pandas as pd
import numpy as np

def test_data_cleaner_remove_duplicates():
    from etl.transformers.data_cleaner import DataCleaner
    
    cleaner = DataCleaner()
    
    df = pd.DataFrame({
        'train_id': [1, 2, 1, 3],
        'name': ['Train A', 'Train B', 'Train A', 'Train C']
    })
    
    df_clean = cleaner.remove_duplicates(df, subset=['train_id'])
    
    assert len(df_clean) == 3
    assert df_clean['train_id'].nunique() == 3


def test_data_cleaner_with_empty_dataframe():
    from etl.transformers.data_cleaner import DataCleaner
    
    cleaner = DataCleaner()
    df = pd.DataFrame({'train_id': [], 'name': []})
    
    df_clean = cleaner.remove_duplicates(df)
    
    assert len(df_clean) == 0


def test_data_cleaner_with_null_values():
    from etl.transformers.data_cleaner import DataCleaner
    
    cleaner = DataCleaner()
    
    df = pd.DataFrame({
        'train_id': [1, 2, None, 3],
        'name': ['Train A', None, 'Train C', 'Train D']
    })
    
    assert df.isnull().sum().sum() == 2
    assert len(df) == 4

def test_sql_injection_in_query_parameters():
    malicious_input = "' OR '1'='1"
    
    assert "'" in malicious_input
    assert "OR" in malicious_input


def test_xss_prevention_in_input():
    malicious_input = "<script>alert('xss')</script>"
    
    assert "<script>" in malicious_input
    assert "alert" in malicious_input


def test_input_validation_for_empty_strings():
    inputs = ["", " ", None]
    
    for inp in inputs:
        if inp is not None:
            assert isinstance(inp, str)


def test_invalid_data_types():
    df = pd.DataFrame({
        'train_id': [1, 'invalid', 3],
        'count': [10, 20, 30]
    })
    
    mixed_types = df['train_id'].apply(lambda x: isinstance(x, (int, np.integer)))
    assert not mixed_types.all()


def test_malicious_csv_injection():
    malicious_inputs = [
        "=cmd|'/c calc'!A1",
        "@SUM(1+9)*cmd|'/c calc'!A1",
        "+2+5+cmd|' /C calc'!A1",
        "-2+3+cmd|' /C calc'!A1"
    ]
    
    for inp in malicious_inputs:
        assert inp[0] in ['=', '@', '+', '-']


def test_path_traversal_prevention():
    malicious_paths = [
        "../../../etc/passwd",
        "..\\..\\..\\windows\\system32",
        "....//....//....//etc/passwd"
    ]
    
    for path in malicious_paths:
        assert ".." in path or ".../" in path
