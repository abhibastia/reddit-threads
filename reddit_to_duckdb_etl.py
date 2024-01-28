import argparse
import configparser
import logging
from extract.reddit_extractor import RedditDataExtractor
from transform.reddit_data_transformer import RedditDataTransformer
from load.duckdb_loader import DuckDBDataLoader


class RedditDuckDBETLPipeline:
    """
    Orchestrates the ETL process for Reddit data.
    """

    def __init__(self, config):
        # Initialize extractor, transformer, and loader with configuration settings
        self.config = config
        self.extractor = RedditDataExtractor(config)
        self.transformer = RedditDataTransformer(config)
        self.loader = DuckDBDataLoader(config)

    def run(self, start_date, no_of_threads):
        """
        Executes the ETL process.
        """
        try:
            # Extract data
            extracted_data = self.extractor.extract_posts_data(start_date, no_of_threads)
            logging.info("Reddit data extracted successfully.")
            logging.debug(f"Total no of rows-extracted_data: {len(extracted_data)}")


            # Apply transformations
            transformed_data = self.transformer.transform(extracted_data)
            logging.info("Data transformation completed.")
            logging.debug(f"Total no of rows-transformed_data: {len(transformed_data)}")

            # Load data
            self.loader.load_dataframe_to_table(transformed_data)
            logging.info("Data loaded into DuckDB successfully.")

        except Exception as error:
            logging.error(f"Error in ETL pipeline: {error}")
            raise


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
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="ETL Pipeline for Reddit Data")
    parser.add_argument("--start_date", required=True, help="Start date for data extraction")
    parser.add_argument("--no_of_threads", type=int, default=2000, help="Number of threads to extract")
    args = parser.parse_args()

    # Load configuration
    config = configparser.ConfigParser()
    config.read('configs/config.ini')

    # Setup logging
    log_level = config.get('logging', 'level', fallback='INFO')
    setup_logging(log_level)

    logging.debug(f"Arguments provided are start_date :{args.start_date} and no_of_threads:{args.no_of_threads}")

    # Run the ETL pipeline.
    try:
        etl_pipeline = RedditDuckDBETLPipeline(config)
        etl_pipeline.run(args.start_date, args.no_of_threads)
    except Exception as e:
        logging.error(f"Error in main ETL process: {e}")
        raise
