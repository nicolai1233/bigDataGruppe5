from Country import Country


class CountryHolder:
    country_list = []
    none_model = None

    def add(self, tweet):
        # handle no place data
        if tweet.place is None:
            if self.none_model is None:
                c = Country(None, [tweet])
                self.country_list.append(c)
                self.none_model = c
            else:
                self.none_model.tweets.append(tweet)
            return

        # find that country model
        for c in self.country_list:
            if c.name == tweet.place.country_code:
                # add the tweet to the country
                c.tweets.append(tweet)
                return
        # if the country model does not exists create one
        c = Country(tweet.place.country_code, [tweet])
        # add to list
        self.country_list.append(c)

    def clear(self):
        self.country_list = []
        self.none_model = None

    def get_raw_data(self):
        res = []
        for c in self.country_list:
            res.extend(c.tweets)
        return res
