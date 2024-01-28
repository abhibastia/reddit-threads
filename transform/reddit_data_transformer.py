import logging
from datetime import datetime

import pandas as pd


class RedditDataTransformer:
    """
    Transforms data within a pandas DataFrame.
    """

    def __init__(self, config):
        """
        Initializes the DataFrameTransformer with the given configuration.
        """
        self.date_column = config['transformation']['date_column']
        self.default_values = config['default_values']
        self.expected_schema = config['schema']
        logging.info("DataFrameTransformer initialized.")

    def transform(self, dataframe):
        """
        Apply all necessary transformations to the DataFrame.
        """
        logging.info("Starting data transformation.")
        # Handle missing values
        dataframe = self.fill_missing_values(dataframe)

        # Add date columns
        dataframe = self.add_datetime_columns(dataframe)
        dataframe['stored_at'] = datetime.now()

        # Validate DataFrame schema
        if self.validate_schema(dataframe):
            logging.info("Data transformation completed.")
            return dataframe
        else:
            logging.error("DataFrame schema validation failed.")

    def add_datetime_columns(self, dataframe):
        """
        Adds datetime related columns to the DataFrame.
        """
        if self.date_column in dataframe.columns:
            dataframe['created_datetime'] = pd.to_datetime(dataframe[self.date_column], unit='s')
            dataframe['created_date'] = dataframe['created_datetime'].dt.date
            dataframe['day'] = dataframe['created_datetime'].dt.day_name()
            dataframe['month'] = dataframe['created_datetime'].dt.month_name()
            dataframe['week_of_year'] = dataframe['created_datetime'].dt.isocalendar().week
            logging.info("Datetime columns added to the DataFrame.")
        else:
            logging.warning(f"Column '{self.date_column}' not found in the DataFrame.")
        return dataframe

    def fill_missing_values(self, dataframe):
        """
        Fills missing values in the DataFrame based on provided configuration.
        """
        for column, default_value in self.default_values.items():
            if dataframe[column].isnull().any():
                logging.info(f"Column '{column}' contains missing values. Filling with default value: {default_value}.")
                dataframe[column] = dataframe[column].fillna(default_value)
        return dataframe

    def validate_schema(self, dataframe):
        """
        Validates the DataFrame's schema.
        """
        for column, expected_dtype in self.expected_schema.items():
            if column not in dataframe.columns or dataframe[column].dtype != expected_dtype:
                logging.warning(
                    f"Column '{column}' does not match expected dtype '{expected_dtype}'. Found: {dataframe[column].dtype if column in dataframe.columns else 'None'}")
                return False
        logging.info("DataFrame Schema is validated.")
        return True
