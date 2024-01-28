import logging
from connector.duckdb_connector import DuckDBConnector


class DuckDBDataLoader:
    """
    Loads data into a DuckDB table.
    """

    def __init__(self, config):
        """
        Initializes the DuckDBDataLoader with the given configuration.
        """
        self.db_connector = DuckDBConnector(config)
        self.target_table_name = config['duckdb']['table_name']
        logging.info(f"DuckDBDataLoader initialized for table: {self.target_table_name}")

    def load_dataframe_to_table(self, dataframe):
        """
        Loads data from a pandas DataFrame into a DuckDB table.
        """
        connection = None
        try:
            logging.debug(f"Total no of rows in dataframe before loading data to duckdb: {len(dataframe)}")
            logging.debug(f"Target Table Name :{self.target_table_name}")
            logging.debug(f"Target database Name:{self.db_connector.duckdb_connection_string}")

            connection = self.db_connector.create_connection()

            # Drop the existing table if it exists
            connection.execute(f"DROP TABLE IF EXISTS {self.target_table_name};")

            # Create table with the new data from the DataFrame
            temporary_table_name = 'temp_reddit_posts'
            connection.register(temporary_table_name, dataframe)
            logging.info(f"Loading data into DuckDB table '{self.target_table_name}'")
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS {self.target_table_name} AS SELECT * FROM {temporary_table_name};")
            connection.unregister(temporary_table_name)
            logging.info(f"Data loaded successfully into DuckDB table '{self.target_table_name}'")
        except Exception as e:
            logging.error(f"Error during data loading operation: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def query_table(self, query):
        """Run a SQL query and return the results."""
        connection = None
        try:
            connection = self.db_connector.create_connection()
            query_result = connection.execute(query).fetchdf()
            return query_result
        except Exception as e:
            logging.error(f"Error during query operation: {e}")
            raise
        finally:
            if connection:
                connection.close()
