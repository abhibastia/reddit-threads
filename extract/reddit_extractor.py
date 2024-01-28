import logging
import praw
import pandas as pd
import datetime
from connector.reddit_connector import RedditAPIConnector

class RedditDataExtractor:
    """
    Extracts data from Reddit using the PRAW API.
    """

    def __init__(self, config):
        """
        Initializes the RedditDataExtractor with the given configuration.
        """
        self.connector = RedditAPIConnector(config)
        self.reddit_client = self.connector.create_reddit_client()
        self.subreddit = config['reddit']['subreddit']
        self.target_columns = config['reddit']['columns'].split(',')
        logging.info("RedditDataExtractor initialized.")

    def extract_posts_data(self, start_date, number_of_threads):
        """
        Extracts posts data from the 'dataengineering' subreddit from a given start date.
        """
        subreddit = self.reddit_client.subreddit(self.subreddit)

        # Convert start_date to Unix timestamp
        start_timestamp = datetime.datetime.strptime(start_date, '%Y-%m-%d').timestamp()

        if logging.getLogger().isEnabledFor(logging.DEBUG):
            for submission in subreddit.new(limit=1):
                submission_info = f"Title: {submission.title}\n"
                submission_info += "\n".join([f"{key}: {value}" for key, value in vars(submission).items()])
                submission_info += "\n" + "-" * 30  # Separator for each submission
                logging.debug(submission_info)

        # Fetch posts
        posts = []
        for submission in subreddit.new(limit=number_of_threads):
            try:
                if submission.created_utc >= start_timestamp:
                    post_data = [getattr(submission, column, None) for column in self.target_columns]
                    posts.append(post_data)
            except praw.exceptions.PRAWException as e:
                logging.error(f"Error fetching posts from Reddit: {e}")
                continue  # Skip this submission and continue with the next

        # Create DataFrame
        posts_data = pd.DataFrame(posts, columns=self.target_columns)
        logging.info(f"{len(posts_data)} posts extracted from Reddit.")
        return posts_data
