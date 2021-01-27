# coding=utf-8

import glob
import gzip
import json
import os
import re
from collections import defaultdict
from hoover.users import get_user_ids


class ExtractRetweets(object):
    def __init__(self, infile, indir, outfile, hashtags):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.hashtags = hashtags

        self.n_tweets = 0
        self.n_retweets = 0
        self.n_inretweets = 0
        self.n_quotes = 0
        self.n_inquotes = 0

        self.tweets = {}
        self.retweets = defaultdict(list)
        self.quotes = defaultdict(list)
        self.parent = {}

        self.tids = set()
        self.scan = 0
        self.pattern = re.compile(r'"id_str": "(.*?)"')

    def _filter_by_hashtags(self, json_str):
        # HACK
        return '#remaniement' in json_str.lower()

    def _filter(self, json_str):
        if self._filter_by_hashtags(json_str):
            return self.scan == 0

        r = re.findall(self.pattern, json_str)
        for id_str in r:
            if id_str in self.tids:
                return True
        return False

    def _simple(self, tweet):
        uid = tweet['user']['id']
        internal = uid in self.user_ids
        text = tweet['full_text'] if 'full_text' in tweet else tweet['text']
        return {
            'id': tweet['id_str'],
            'text': text,
            'created_at': tweet['created_at'],
            'user': tweet['user']['screen_name'],
            'user_id': tweet['user']['id'],
            'followers_count': tweet['user']['followers_count'],
            'friends_count': tweet['user']['friends_count'],
            'internal': internal}

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # TEMPORARY HACK!!! (DATE)
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '2020-06.json.gz'))
        file_names += glob.glob(
            os.path.join(self._user_path(user_id), '2020-07.json.gz'))
        return file_names

    def _urtweet(self, tid):
        if tid in self.parent:
            return self._urtweet(self.parent[tid])
        else:
            return self.tweets[tid]

    def _urtweet_contains_hashtags(self, tid):
        urtweet = self._urtweet(tid)
        for hashtag in self.hashtags:
            if hashtag in urtweet['text'].lower():
                return True
        return False

    def _scan(self):
        new = 0
        for i, user_id in enumerate(self.user_ids):
            print('[SCAN {}] processing user {} #{}/{}...'.format(
                self.scan, user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        if self._filter(line):
                            tweet = json.loads(line)
                            tid = tweet['id_str']
                            if tid not in self.tids:
                                self.tids.add(tid)
                                new += 1

                                self.n_tweets += 1
                                if 'retweeted_status' in tweet:
                                    self.n_retweets += 1
                                    rs = tweet['retweeted_status']
                                    ruid = rs['user']['id']
                                    if ruid in self.user_ids:
                                        self.n_inretweets += 1
                                    parent = rs
                                    parent_id = parent['id_str']
                                    self.retweets[parent_id].append(
                                        self._simple(tweet))
                                    self.tweets[parent_id] = self._simple(
                                        parent)
                                elif 'quoted_status' in tweet:
                                    self.n_quotes += 1
                                    qs = tweet['quoted_status']
                                    quid = qs['user']['id']
                                    if quid in self.user_ids:
                                        self.n_inquotes += 1
                                    parent = qs
                                    parent_id = parent['id_str']
                                    self.quotes[parent_id].append(
                                        self._simple(tweet))
                                    self.tweets[parent_id] = self._simple(
                                        parent)
                                    self.parent[tweet['id_str']] = parent_id

                    fields = ['tweets',
                              'retweets', 'inretweets',
                              'quotes', 'inquotes']
                    field_strs = ['# {}: {{}}'.format(field)
                                  for field in fields]
                    info_str = '; '.join(field_strs)
                    print(info_str.format(self.n_tweets,
                                          self.n_retweets, self.n_inretweets,
                                          self.n_quotes, self.n_inquotes))
        return new

    def run(self):
        while self._scan() > 0:
            self.scan += 1

        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for tid in self.quotes:
                if len(self.quotes[tid]) > 0:
                    if self._urtweet_contains_hashtags(tid):
                        tweet = self.tweets[tid]
                        tweet['retweets'] = self.retweets[tid]
                        tweet['quotes'] = self.quotes[tid]
                        f.write('{}\n'.format(
                            json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    hashtags = {'#remaniement', '#RemaniementMinisteriel',
                '#RemaniementDeLaHonte', '#RemaniementMinistériel'}

    hastags = set(hastag.lower() for hastag in hashtags)

    tr = ExtractRetweets(
        'retweet-userids.csv', 'timelines', 'retweets3.json', hashtags)
    tr.run()
