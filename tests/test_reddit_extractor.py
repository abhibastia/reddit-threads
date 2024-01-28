import pandas as pd
from unittest.mock import patch, MagicMock
from extract.reddit_extractor import RedditDataExtractor


@patch('extract.reddit_extractor.RedditAPIConnector')
def test_extract_posts_data(mock_api_connector):
    # Mock configuration
    config = {
        'reddit': {
            'client_id': 'test_id',
            'client_secret': 'test_secret',
            'user_agent': 'test_agent',
            'subreddit': 'test_subreddit',
            'columns': 'id,title,created_utc'
        }
    }

    # Mock the Reddit client and its methods
    mock_reddit = MagicMock()
    mock_api_connector.return_value.create_reddit_client.return_value = mock_reddit

    # Mock subreddit and submissions
    mock_subreddit = MagicMock()
    mock_reddit.subreddit.return_value = mock_subreddit

    mock_submission = MagicMock()
    mock_submission.created_utc = 1609459200
    mock_submission.id = "test_id"
    mock_submission.title = "Test Title"
    mock_subreddit.new.return_value = iter([mock_submission])

    # Initialize RedditDataExtractor
    extractor = RedditDataExtractor(config)

    # Test extract_posts_data
    start_date = '2021-01-01'
    number_of_threads = 1
    result_df = extractor.extract_posts_data(start_date, number_of_threads)

    # Assertions
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 1
    assert result_df.iloc[0]['id'] == "test_id"
    assert result_df.iloc[0]['title'] == "Test Title"

    # Assert praw Reddit was called with correct parameters
    mock_api_connector.return_value.create_reddit_client.assert_called_once()
