from tweepy import OAuthHandler, Stream

from TwitterStreamListener import TweetListener

access_token = '1326854043122233344-VasKbQmR1xzfLWAhZ6s369ziRyDsQt'
access_token_secret = 'ziavkmt13pSNyuOeqZJpwNOpzw9XF7LnQeyntEPMQQAag'
consumer_key = 'ulvOi0se49XC87pVERlMMMMYS'
consumer_secret = '25ZMpaONlxrWUwQhkJy41e7MMUoCONLjiLWzTUyFsEPvyx7Uqq'


class TwitterStream:

    def start_streaming(self, cash):
        twitter_listener = TweetListener(cash)
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        stream = Stream(auth, twitter_listener)
        stream.filter(track=['#COVID19', '#covid19', '#Covid19', '#covid-19', '#Coronavirus'])
        return stream
