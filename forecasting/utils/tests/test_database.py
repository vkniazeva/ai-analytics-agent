import pytest
import pandas as pd
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine

from forecasting.utils.database import get_engine, read_sql, write_sql


# Test get_engine
@patch.dict('os.environ', {
    'DB_HOST': 'localhost',
    'DB_PORT': '5433',
    'DB_NAME': 'testdb',
    'DB_USER': 'testuser',
    'DB_PASSWORD': 'testpass'
})
def test_get_engine_returns_engine():
    engine = get_engine()
    assert engine is not None
    assert str(engine.url).startswith('postgresql+psycopg2://')


@patch.dict('os.environ', {
    'DB_HOST': 'localhost',
    'DB_PORT': '5433',
    'DB_NAME': 'testdb',
    'DB_USER': 'testuser',
    'DB_PASSWORD': 'testpass'
})
def test_get_engine_uses_env_variables():
    engine = get_engine()
    url = str(engine.url)

    assert 'testdb' in url
    assert 'testuser' in url
    assert 'localhost' in url
    assert '5433' in url


@patch.dict('os.environ', {
    'DB_NAME': 'testdb',
    'DB_USER': 'testuser',
    'DB_PASSWORD': 'testpass'
}, clear=True)
def test_get_engine_uses_defaults():
    engine = get_engine()
    url = str(engine.url)

    # Should use default host and port
    assert 'localhost' in url
    assert '5433' in url


# Test read_sql - mock mode
@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.FORECASTING_PATH')
def test_read_sql_mock_mode(mock_path, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "mock"
            }
        }
    }

    # Create temporary CSV file
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)
    mock_path.__truediv__ = lambda self, other: temp_path / other

    # Create interim_files directory and test CSV
    interim_dir = temp_path / "interim_files"
    interim_dir.mkdir(parents=True, exist_ok=True)

    test_df = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'quantity': [5, 3]
    })
    csv_path = interim_dir / "test_table.csv"
    test_df.to_csv(csv_path, index=False)

    with patch('forecasting.utils.database.FORECASTING_PATH', temp_path):
        result = read_sql("SELECT * FROM test", "test_table")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'item_id' in result.columns

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_read_sql_database_mode(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    # Mock engine and pd.read_sql
    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    with patch('pandas.read_sql', return_value=test_df) as mock_read_sql:
        result = read_sql("SELECT * FROM table")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    mock_read_sql.assert_called_once()


# Test write_sql - mock mode
@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.FORECASTING_PATH')
def test_write_sql_mock_mode(mock_path, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "mock"
            }
        }
    }

    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)

    # Create interim_files directory
    interim_dir = temp_path / "interim_files"
    interim_dir.mkdir(parents=True, exist_ok=True)

    test_df = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'quantity': [5]
    })

    with patch('forecasting.utils.database.FORECASTING_PATH', temp_path):
        write_sql(test_df, "test_table")

    # Check file was created
    csv_path = interim_dir / "test_table.csv"
    assert csv_path.exists()

    # Verify content
    written_df = pd.read_csv(csv_path)
    assert len(written_df) == 1
    assert 'item_id' in written_df.columns

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_write_sql_database_mode_append(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1], 'value': [10]})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        write_sql(test_df, "regular_table")

    mock_to_sql.assert_called_once()
    # Check that if_exists is 'append' for regular tables
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs['if_exists'] == 'append'


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_write_sql_database_mode_replace_lookup(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1], 'value': [10]})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        write_sql(test_df, "lookup_hist_avg")

    mock_to_sql.assert_called_once()
    # Check that if_exists is 'replace' for lookup tables
    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs['if_exists'] == 'replace'


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_write_sql_uses_correct_schema(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1]})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        write_sql(test_df, "test_table", schema="custom_schema")

    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs['schema'] == 'custom_schema'


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_write_sql_default_schema(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1]})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        write_sql(test_df, "test_table")

    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs['schema'] == 'forecasting'


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_write_sql_no_index(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1]})

    with patch.object(pd.DataFrame, 'to_sql') as mock_to_sql:
        write_sql(test_df, "test_table")

    call_kwargs = mock_to_sql.call_args[1]
    assert call_kwargs['index'] is False


# Test edge cases
@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.FORECASTING_PATH')
def test_write_sql_empty_dataframe_mock(mock_path, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "mock"
            }
        }
    }

    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir)
    interim_dir = temp_path / "interim_files"
    interim_dir.mkdir(parents=True, exist_ok=True)

    empty_df = pd.DataFrame()

    with patch('forecasting.utils.database.FORECASTING_PATH', temp_path):
        write_sql(empty_df, "empty_table")

    csv_path = interim_dir / "empty_table.csv"
    assert csv_path.exists()

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)


@patch('forecasting.utils.database.return_config')
@patch('forecasting.utils.database.get_engine')
def test_read_sql_calls_engine(mock_engine, mock_config):
    mock_config.return_value = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        }
    }

    mock_engine_instance = MagicMock()
    mock_engine.return_value = mock_engine_instance

    test_df = pd.DataFrame({'id': [1]})

    with patch('pandas.read_sql', return_value=test_df) as mock_read_sql:
        result = read_sql("SELECT * FROM test")

    # Verify get_engine was called
    mock_engine.assert_called()
    # Verify pd.read_sql was called with the engine
    assert mock_read_sql.called
