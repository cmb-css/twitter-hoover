import json
import gzip
from twython import TwythonError
from hoover.auth import twython_from_key_and_auth
from hoover.rate_control import RateControl


def json_split(json_str):
    if len(json_str.split('}{')) == 1:
        return [json_str.strip()]
    parts = []
    depth = 0
    part = ''
    for i, c in enumerate(json_str.strip()):
        part += c
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
        if depth == 0:
            parts.append(part)
            part = ''
    return parts


class Hydrate(RateControl):
    def __init__(self, infile, outfile, errfile, key_file, auth_file):
        super().__init__(rate_limit=500)
        self.infile = infile
        self.outfile = outfile
        self.errfile = errfile
        self.twitter = twython_from_key_and_auth(key_file, auth_file)
        self.retrieved = 0
        self.lost = 0

    def get_tweets(self, tweet_ids):
        ids = ','.join(tweet_ids)
        try:
            tweets = self.twitter.lookup_status(id=ids,
                                                include_rt=True,
                                                tweet_mode='extended')
            return tweets
        except TwythonError as e:
            print('*** {}'.format(len(tweet_ids)))
            print('ERROR: {}'.format(e))
            with open(self.errfile, 'a') as file:
                file.write('ERROR: {}\n'.format(e))
            return []

    def _hydrate_and_write(self, truncated_ids, non_truncated_tweets):
        if len(truncated_ids) > 0:
            self.pre_request()
            tweets = self.get_tweets(truncated_ids)
        else:
            tweets = []
        self.retrieved += len(tweets)
        self.lost += len(truncated_ids) - len(tweets)
        print('{} tweets retrieved, {} tweets lost.'.format(
            self.retrieved, self.lost))
        tweets += non_truncated_tweets
        tweets = sorted(tweets, key=lambda k: k['id'])
        with gzip.open(self.outfile, 'at') as f:
            for tweet in tweets:
                f.write('{}\n'.format(json.dumps(tweet)))

    def retrieve(self):
        ids = []
        tweets = []
        with gzip.open(self.infile, 'rt') as f:
            for line in f:
                try:
                    tid = int(line.strip())
                    ids.append(str(tid))
                    if len(ids) >= 100:
                        self._hydrate_and_write(ids, tweets)
                        ids = []
                        tweets = []
                except ValueError:
                    for json_str in json_split(line):
                        try:
                            tweet = json.loads(json_str)
                            if tweet['truncated']:
                                ids.append(tweet['id_str'])
                            else:
                                tweets.append(tweet)
                        except Exception as e:
                            print('ERROR: {}'.format(e))
                            with open(self.errfile, 'a') as file:
                                file.write('ERROR: {}\n'.format(e))
                        if len(ids) >= 100 or len(tweets) >= 100000:
                            self._hydrate_and_write(ids, tweets)
                            ids = []
                            tweets = []
        self._hydrate_and_write(ids, tweets)


def hydrate_file(key_file, auth_file, infile, outfile, errfile):
    hydrate = Hydrate(infile, outfile, errfile, key_file, auth_file)
    hydrate.retrieve()
