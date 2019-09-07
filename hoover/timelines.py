import os
import time
import csv
import json
from twython import Twython, TwythonError
from hoover.auth import read_key_and_secret, read_token_secret_pin
from hoover.snowflake import *


def get_user_ids(file):
    user_ids = []
    with open(file) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            user_ids.append(row[0])
    return user_ids


def last_line(file):
    try:
        with open(file, 'rb') as f:
            f.readline()
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b"\n":
                f.seek(-2, os.SEEK_CUR)
            return f.readline().decode().strip()
    except OSError:
        return None


class Timelines():
    def __init__(self, infile, outdir, errfile, min_utc, retweets,
                 app_key, app_secret, oauth_token, oauth_token_secret):
        self.user_ids = get_user_ids(infile)
        self.outdir = outdir
        self.errfile = errfile
        self.retweets = retweets
        self.twitter = Twython(app_key, app_secret,
                               oauth_token, oauth_token_secret)
        self.min_id = utc2snowflake(min_utc)
        self.max_id = None
        self.iter = 0
        self.requests = 0
        self.start_t = None
        self.reqs_per_day = 0.

    def get_timeline(self, user_id, max_id):
        self.requests += 1
        try:
            timeline = self.twitter.get_user_timeline(user_id=user_id,
                                                      include_rt=self.retweets,
                                                      max_id=max_id,
                                                      count=200)
            return timeline
        except TwythonError as e:
            print('ERROR: {}'.format(e))
            with open(self.errfile, 'a') as file:
                file.write('ERROR: {}\n'.format(e))
            return None

    def _user_path(self, user_id):
        return os.path.join(self.outdir, '{}.json'.format(str(user_id)))

    def _user_last_tweet_id(self, user_id):
        ll = last_line(self._user_path(user_id))
        if ll is None:
            return None
        else:
            tweet = json.loads(ll)
            return tweet['id']

    def _retrieve(self):
        for i, user_id in enumerate(self.user_ids):
            print('[iter: {}] processing user #{}/{}...'.format(
                self.iter, i, len(self.user_ids)))
            tweets = []
            min_id = self._user_last_tweet_id(user_id)
            if min_id is None:
                min_id = self.min_id
            max_id = self.max_id
            finished = False
            while not finished:
                if self.reqs_per_day > 99000.:
                    time.sleep(1)
                timeline = self.get_timeline(user_id, max_id - 1)
                if timeline:
                    print('{} tweets received'.format(str(len(timeline))))
                    for tweet in timeline:
                        max_id = tweet['id']
                        if tweet['id'] > min_id:
                            tweets.append(tweet)
                        else:
                            finished = True
                else:
                    finished = True
            # write to file
            file = self._user_path(user_id)
            with open(file, 'a') as f:
                for tweet in reversed(tweets):
                    f.write('{}\n'.format(json.dumps(tweet)))
            print('{} tweets found.'.format(len(tweets)))

            delta_t = (time.time() - self.start_t) / (60. * 60. * 24.)
            self.reqs_per_day = self.requests / delta_t
            print('{} requests/day'.format(self.reqs_per_day))
            print('{} users/day'.format(i / delta_t))

    def retrieve(self):
        self.start_t = time.time()
        while True:
            self.max_id = utc2snowflake(utcnow())
            self._retrieve()
            self.iter += 1


def retrieve_timelines(key_file, auth_file,
                       infile, outdir, errfile,
                       min_utc, retweets):
    app_key, app_secret = read_key_and_secret(key_file)
    oauth_token, oauth_token_secret = read_token_secret_pin(auth_file)

    timelines = Timelines(infile, outdir, errfile, min_utc, retweets,
                          app_key, app_secret, oauth_token, oauth_token_secret)
    timelines.retrieve()
