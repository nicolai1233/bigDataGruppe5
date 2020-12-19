class DataPoint:
    
    def __init__(self, covidCases, tweets, date):
        self.date = date
        self.tweets = tweets
        self.covidCases = covidCases
