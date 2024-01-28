import pandas as pd
from unittest.mock import patch, MagicMock
from load.duckdb_loader import DuckDBDataLoader


@patch('load.duckdb_loader.DuckDBConnector')
def test_load_dataframe_to_table(mock_connector_class):
    # Create a mock configuration
    config = {
        'duckdb': {
            'connection_string': 'dummy_string',
            'table_name': 'test_table'
        }
    }

    # Create a mock connection object
    mock_connection = MagicMock()
    mock_connector_instance = MagicMock()
    mock_connector_instance.create_connection.return_value = mock_connection
    mock_connector_class.return_value = mock_connector_instance

    # Initialize the DuckDBDataLoader
    dataloader = DuckDBDataLoader(config)

    # Create a test DataFrame
    test_df = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})

    # Call the load_dataframe_to_table method
    dataloader.load_dataframe_to_table(test_df)

    # Assertions
    # Check if the methods are called on the mock connection
    mock_connection.register.assert_called_once_with('temp_reddit_posts', test_df)
    mock_connection.execute.assert_called()
    mock_connection.unregister.assert_called_once_with('temp_reddit_posts')

