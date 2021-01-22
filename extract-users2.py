# coding=utf-8

import os
import glob
import gzip
import json
from hoover.users import get_user_ids


class ExtractUsers(object):
    def __init__(self, infile, indir, outfile, hashtags):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.hashtags = hashtags

        self.users = {}
        self.n_tweets = 0
        self.hashtag_counts = {}

        for ht in self.hashtags:
            self.hashtag_counts[ht] = 0

    def _filter_by_hashtags(self, json_str):
        for hashtag in self.hashtags:
            if hashtag in json_str.lower():
                return True
        return False

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # TEMPORARY HACK!!! (DATE)
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '2020-06.json.gz'))
        file_names += glob.glob(
            os.path.join(self._user_path(user_id), '2020-07.json.gz'))
        return file_names

    def _count_hashtag(self, user, hashtag):
        uid = user['id']
        user = user['screen_name']

        if uid not in self.users:
            self.users[uid] = {'screen_name': user,
                               'hashtags': {},
                               'tweets': 0}
            for ht in self.hashtags:
                self.users[uid]['hashtags'][ht] = 0

        self.users[uid]['hashtags'][hashtag] += 1
        self.hastag_counts[hashtag] += 1

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        if self._filter_by_hashtags(line):
                            tweet = json.loads(line)
                            self.n_tweets += 1
                            if ('entities' in tweet and
                                    'hashtags' in tweet['entities']):
                                hashtags = [hashtag['text']
                                            for hashtag
                                            in tweet['entities']['hashtags']]
                                matches = False
                                for ht1 in hashtags:
                                    for ht2 in self.hashtags:
                                        if ht1.lower() == ht2.lower():
                                            self._count_hashtag(tweet['user'],
                                                                ht2)
                                            matches = True
                                if matches:
                                    uid = tweet['user']['id']
                                    self.users[uid]['tweets'] += 1
                                    self.n_tweets += 1

            print('tweets: {}; users: {}'.format(
                self.n_tweets, len(self.users)))
            print(self.hashtag_counts)

        with open(self.outfile, 'wt') as f:
            for uid in self.users:
                user = self.users[uid]
                if len(user['tweets']) > 1:
                    user['id'] = uid
                    f.write('{}\n'.format(json.dumps(user)))


if __name__ == '__main__':
    hashtags = ['#remaniement', '#RemaniementMinisteriel',
                '#RemaniementDeLaHonte', '#RemaniementMinistériel']

    hastags = list(hastag.lower() for hastag in hashtags)

    tr = ExtractUsers(
        'retweet-userids.csv', 'timelines', 'users2.json', hashtags)
    tr.run()
