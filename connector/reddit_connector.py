import logging
import praw


class RedditAPIConnector:
    """
    Manages the connection to the Reddit API using praw.
    """

    def __init__(self, config):
        """
        Initializes the RedditAPIConnector with the given configuration.
        """
        self.reddit_client_id = config['reddit']['client_id']
        self.reddit_client_secret = config['reddit']['client_secret']
        self.reddit_user_agent = config['reddit']['user_agent']

        try:
            self.reddit_client = self.create_reddit_client()
            logging.info("RedditAPIConnector initialized and connected to Reddit.")
        except Exception as e:
            logging.error(f"Error initializing Reddit API connection: {e}")
            raise

    def create_reddit_client(self):
        """
        Creates and returns a Reddit client instance.
        """
        try:
            return praw.Reddit(client_id=self.reddit_client_id,
                               client_secret=self.reddit_client_secret,
                               user_agent=self.reddit_user_agent)
        except Exception as e:
            logging.error(f"Error creating Reddit client: {e}")
            raise
