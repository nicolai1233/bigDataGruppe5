from tweepy import StreamListener


class TweetListener(StreamListener):

    def __init__(self, cash, api=None):
        super().__init__(api)
        self.cash = cash

    def on_status(self, status):
        self.cash.add(status)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
