import configparser
import logging
from load.duckdb_loader import DuckDBDataLoader


class DuckDBQueryRunner:
    """
    Executes queries on a DuckDB database based on configuration.
    """

    def __init__(self, config):
        """
        Initializes the query runner with the given configuration file.
        """
        self.config = config
        self.data_loader = DuckDBDataLoader(self.config)

    def run_queries(self):
        """
        Runs the queries specified in the configs file.
        """
        if 'queries' not in self.config:
            raise ValueError("No queries found in the configs file.")

        for query_name, query in self.config['queries'].items():
            try:
                query_result = self.data_loader.query_table(query)
                logging.info(f"Results for {query_name}")
                logging.info(f"\n {query_result.to_string()}")
            except Exception as e:
                logging.error(f"Error executing {query_name}: {e}")


def setup_logging(log_level):
    """
    Sets up the logging configuration.
    """
    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        logging.warning(f"Invalid log level: {log_level}. Defaulting to INFO.")
        level = logging.INFO
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')


if __name__ == "__main__":
    # Load configuration
    config = configparser.ConfigParser()
    config.read('configs/config.ini')

    # Setup logging
    log_level = config.get('logging', 'level', fallback='INFO')
    setup_logging(log_level)

    # Run queries from duckdb table
    runner = DuckDBQueryRunner(config)
    runner.run_queries()
