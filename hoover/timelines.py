import os
import csv
import json
from twython import TwythonError
from hoover.auth import twython_from_key_and_auth
from hoover.snowflake import *
from hoover.rate_control import RateControl
from hoover.users import Users


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


class Timelines(RateControl):
    def __init__(self, infile, screen_name, outdir, errfile, min_utc, retweets,
                 key_file, auth_file):
        super().__init__()
        if infile is not None:
            self.user_ids = get_user_ids(infile)
        elif screen_name is not None:
            user_id = Users(key_file, auth_file).screen_name2id(screen_name)
            self.user_ids = [user_id]
        else:
            raise RuntimeError('Provide either infile or screen_name.')
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
            file = self._user_path(user_id)
            with open(file, 'a') as f:
                for tweet in reversed(tweets):
                    f.write('{}\n'.format(json.dumps(tweet)))
            print('{} tweets found.'.format(len(tweets)))

            if self.delta_t > 0.:
                print('{} requests/day'.format(self.reqs_per_day))
                print('{} users/day'.format(
                    (self.iter * len(self.user_ids) + i) / self.delta_t))

    def retrieve(self):
        while True:
            self.max_id = utc2snowflake(utcnow())
            self._retrieve()
            self.iter += 1


def retrieve_timelines(key_file, auth_file,
                       infile, screen_name, outdir, errfile,
                       min_utc, retweets):
    timelines = Timelines(infile, screen_name, outdir, errfile, min_utc,
                          retweets, key_file, auth_file)
    timelines.retrieve()
