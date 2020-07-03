import sys
import os
import glob
import json
import gzip
from collections import defaultdict
from twython import TwythonError
from hoover.auth import twython_from_key_and_auth
from hoover.snowflake import *
from hoover.rate_control import RateControl
from hoover.users import Users, get_user_ids
from datetime import datetime


def last_line(file):
    try:
        with gzip.open(file, 'rt') as f:
            last_line = None
            for line in f:
                last_line = line
            return last_line
    except OSError:
        return None


class Timelines(RateControl):
    def __init__(self, infile, user, outdir, errfile, min_utc, retweets,
                 key_file, auth_file):
        super().__init__(rate_limit=900)
        if infile is not None:
            self.user_ids = get_user_ids(infile)
        elif user is not None:
            user_id = Users(key_file, auth_file).user2id(user)
            self.user_ids = [user_id]
        else:
            raise RuntimeError('Provide either --infile or --user.')
        self.outdir = outdir
        self.errfile = errfile
        self.retweets = retweets
        self.twitter = twython_from_key_and_auth(key_file, auth_file)
        self.min_id = utc2snowflake(min_utc)
        self.max_id = None
        self.iter = 0

    def get_timeline(self, user_id, max_id):
        try:
            timeline = self.twitter.get_user_timeline(user_id=user_id,
                                                      include_rt=self.retweets,
                                                      max_id=max_id,
                                                      count=200,
                                                      tweet_mode='extended')
            return timeline
        except TwythonError as e:
            print('ERROR: {}'.format(e))
            with open(self.errfile, 'a') as file:
                file.write('ERROR: {}\n'.format(e))
            return None

    def _user_path(self, user_id):
        return os.path.join(self.outdir, str(user_id))

    def _cur_file(self, user_id):
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '*.json.gz'))
        max_date_month = 0
        latest_file = None
        for file_name in file_names:
            # TODO: hack
            if 'hydrated' not in file_name:
                base = os.path.basename(file_name)
                base = base.split('.')[0]
                date_month = int(base.replace('-', ''))
                if date_month > max_date_month:
                    max_date_month = date_month
                    latest_file = file_name
        print('latest_file: {}'.format(latest_file))
        return latest_file

    def _user_last_tweet_id(self, user_id):
        cur_file = self._cur_file(user_id)
        if cur_file is None:
            return None
        ll = last_line(cur_file)
        if ll is None:
            return None
        else:
            tweet = json.loads(ll)
            print('latest_time: {}'.format(tweet['created_at']))
            return tweet['id']

    def _retrieve(self):
        for i, user_id in enumerate(self.user_ids):
            print('[iter: {}] processing user {} #{}/{}...'.format(
                self.iter, user_id, i, len(self.user_ids)))
            tweets = []
            min_id = self._user_last_tweet_id(user_id)
            if min_id is None:
                min_id = self.min_id
            max_id = self.max_id
            finished = False
            while not finished:
                self.pre_request()
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
            tweets_months = defaultdict(list)
            for tweet in reversed(tweets):
                ts = str2utc(tweet['created_at'])
                month_year = datetime.utcfromtimestamp(ts).strftime('%Y-%m')
                tweets_months[month_year].append(json.dumps(tweet))
            for month_year in tweets_months:
                outfile = '{}/{}.json.gz'.format(
                    self._user_path(user_id), month_year)
                with gzip.open(outfile, 'at') as of:
                    for tweet_json in tweets_months[month_year]:
                        print(tweet_json, file=of)

            print('{} tweets found.'.format(len(tweets)))

            if self.delta_t:
                print('{} requests/day'.format(self.reqs_per_day))
                print('{} users/day'.format(
                    (self.iter * len(self.user_ids) + i) / self.delta_t))

    def retrieve(self):
        while True:
            self.max_id = utc2snowflake(utcnow())
            self._retrieve()
            self.iter += 1


def retrieve_timelines(key_file, auth_file,
                       infile, user, outdir, errfile,
                       min_utc, retweets):
    timelines = Timelines(infile, user, outdir, errfile, min_utc,
                          retweets, key_file, auth_file)
    timelines.retrieve()
