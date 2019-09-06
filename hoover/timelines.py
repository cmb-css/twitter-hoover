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
    def __init__(self, infile, outdir, errfile, min_utc,
                 app_key, app_secret, oauth_token, oauth_token_secret):
        self.user_ids = get_user_ids(infile)
        self.outdir = outdir
        self.errfile = errfile
        self.min_utc = min_utc
        self.twitter = Twython(app_key, app_secret,
                               oauth_token, oauth_token_secret)
        self.max_id = utc2snowflake(utcnow())

    def get_timeline(self, user_id, max_id):
        try:
            timeline = self.twitter.get_user_timeline(user_id=user_id,
                                                      include_rts=1,
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

    def retrieve(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user #{}/{}...'.format(i, len(self.user_ids)))
            ntweets = 0
            max_id = self._user_last_tweet_id(user_id)
            if max_id is None:
                max_id = self.max_id
            finished = False
            while not finished:
                time.sleep(1)
                finished = True
                timeline = self.get_timeline(user_id, max_id - 1)
                if timeline:
                    for tweet in timeline:
                        max_id = tweet['id']
                        if str2utc(tweet['created_at']) >= self.min_utc:
                            finished = False
                            ntweets += 1
                            # write to file
                            file = self._user_path(user_id)
                            with open(file, 'a') as f:
                                f.write('{}\n'.format(json.dumps(tweet)))
            print('{} tweets found.'.format(ntweets))


def retrieve_timelines(key_file, auth_file, infile, outdir, errfile, min_utc):
    app_key, app_secret = read_key_and_secret(key_file)
    oauth_token, oauth_token_secret = read_token_secret_pin(auth_file)

    timelines = Timelines(infile, outdir, errfile, min_utc,
                          app_key, app_secret, oauth_token, oauth_token_secret)
    timelines.retrieve()
