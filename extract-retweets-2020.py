# coding=utf-8

import glob
import gzip
import json
import os
from collections import defaultdict
from hoover.users import get_user_ids


class ExtractRetweets(object):
    def __init__(self, infile, indir, outfile):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

        self.n_quotes = 0

        self.tweets = {}
        self.quotes = defaultdict(list)
        self.parent = {}

        self.tids = set()
        self.scan = 0

    def _filter(self, json_str):
        return 'quoted_status' in json_str

    def _simple(self, tweet):
        uid = tweet['user']['id']
        internal = uid in self.user_ids
        text = tweet['full_text'] if 'full_text' in tweet else tweet['text']
        data = {
            'id': tweet['id_str'],
            'text': text,
            'created_at': tweet['created_at'],
            'user': tweet['user']['screen_name'],
            'user_id': tweet['user']['id'],
            'followers_count': tweet['user']['followers_count'],
            'friends_count': tweet['user']['friends_count'],
            'internal': internal,
            'urls': []}
        if 'entities' in tweet:
            if 'urls' in tweet['entities']:
                for url_entity in tweet['entities']['urls']:
                    data['urls'].append(url_entity['expanded_url'])
        return data

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        file_names = []
        for i in range(1, 13):
            file_names += glob.glob(os.path.join(
                self._user_path(user_id), '2020-{:02}.json.gz'.format(i)))
        return file_names

    def _urtweet(self, tid):
        if tid in self.parent:
            return self._urtweet(self.parent[tid])
        else:
            return self.tweets[tid]

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
                            try:
                                tweet = json.loads(line)
                                tid = tweet['id_str']
                                if tid not in self.tids:
                                    self.tids.add(tid)
                                    new += 1
                                    if 'quoted_status' in tweet:
                                        qs = tweet['quoted_status']
                                        quid = qs['user']['id']
                                        if quid in self.user_ids:
                                            self.n_quotes += 1
                                            parent = qs
                                            parent_id = parent['id_str']
                                            self.quotes[parent_id].append(
                                                self._simple(tweet))
                                            self.tweets[parent_id] =\
                                                self._simple(parent)
                                            self.parent[tweet['id_str']] =\
                                                parent_id
                            except json.decoder.JSONDecodeError:
                                pass
                    print('user #: {}; quotes: {}'.format(i, self.n_quotes))
        return new

    def run(self):
        while self._scan() > 0:
            self.scan += 1

        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for tid in self.quotes:
                if len(self.quotes[tid]) > 0:
                    tweet = self.tweets[tid]
                    tweet['quotes'] = self.quotes[tid]
                    f.write('{}\n'.format(
                        json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    tr = ExtractRetweets(
        'eu-elections-userids.csv', 'timelines', 'retweets-2020.json')
    tr.run()
