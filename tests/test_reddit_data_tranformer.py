import pandas as pd
from datetime import datetime
from transform.reddit_data_transformer import RedditDataTransformer


class TestRedditDataTransformer:
    @staticmethod
    def get_sample_config():
        return {
            'transformation': {
                'date_column': 'created_utc'
            },
            'default_values': {
                'title': 'NoTitle',
                'author': 'UnknownAuthor'
            },
            'schema': {
                'title': 'object',
                'author': 'object',
                'created_utc': 'float64',
                'created_datetime': 'datetime64[ns]',
                'created_date': 'object',
                'day': 'object',
                'month': 'object',
                'week_of_year': 'int64',
                'stored_at': 'datetime64[ns]'
            }
        }

    def test_add_datetime_columns(self):
        config = self.get_sample_config()
        transformer = RedditDataTransformer(config)

        test_data = {'created_utc': [1609459200.0, 1609545600.0]}  # Unix timestamps for two different days
        df = pd.DataFrame(test_data)

        result_df = transformer.add_datetime_columns(df)

        # Assertions
        assert 'created_datetime' in result_df.columns
        assert 'created_date' in result_df.columns
        assert 'day' in result_df.columns
        assert 'month' in result_df.columns
        assert 'week_of_year' in result_df.columns

        # Check the values in the new columns for the first row
        assert result_df['created_datetime'].iloc[0] == pd.Timestamp('2021-01-01')
        assert result_df['created_date'].iloc[0] == pd.Timestamp('2021-01-01').date()
        assert result_df['day'].iloc[0] == 'Friday'
        assert result_df['month'].iloc[0] == 'January'
        assert result_df['week_of_year'].iloc[0] == 53  # Week number for 2021-01-01

        # Check the values in the new columns for the second row
        assert result_df['created_datetime'].iloc[1] == pd.Timestamp('2021-01-02')
        assert result_df['created_date'].iloc[1] == pd.Timestamp('2021-01-02').date()
        assert result_df['day'].iloc[1] == 'Saturday'
        assert result_df['month'].iloc[1] == 'January'
        assert result_df['week_of_year'].iloc[1] == 53  # Week number for 2021-01-02


    def test_fill_missing_values(self):
        config = self.get_sample_config()
        transformer = RedditDataTransformer(config)

        test_data = {'title': [None], 'author': [None]}  # Both title and author are missing
        df = pd.DataFrame(test_data)

        result_df = transformer.fill_missing_values(df)

        # Assertions
        assert result_df['title'].iloc[0] == 'NoTitle'
        assert result_df['author'].iloc[0] == 'UnknownAuthor'

    def test_validate_schema(self):
        config = self.get_sample_config()
        transformer = RedditDataTransformer(config)

        # DataFrame with correct schema
        test_data = {
            'title': ['Post'],
            'author': ['Author'],
            'created_utc': [1609459200.0],
            'created_datetime': [datetime.now()],
            'created_date': [datetime.now().date()],
            'day': ['Monday'],
            'month': ['January'],
            'week_of_year': [1],
            'stored_at': [datetime.now()]
        }
        df = pd.DataFrame(test_data)

        assert transformer.validate_schema(df) == True

        # DataFrame with incorrect schema
        incorrect_df = pd.DataFrame({'title': [1]})  # Incorrect data type
        assert transformer.validate_schema(incorrect_df) == False
