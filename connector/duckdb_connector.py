import logging
import duckdb


class DuckDBConnector:
    """
    Manages connections to a DuckDB database.
    """

    def __init__(self, config):
        """
        Initializes the DuckDBConnector with the given configuration.
        """
        self.duckdb_connection_string = config['duckdb']['connection_string']
        logging.info("DuckDBConnector initialized with connection string.")

    def create_connection(self):
        """
        Creates and returns a connection to the DuckDB database.
        """
        try:
            connection = duckdb.connect(self.duckdb_connection_string)
            logging.info("Connected to DuckDB successfully.")
            return connection
        except Exception as e:
            logging.error(f"Error connecting to DuckDB: {e}")
            raise
