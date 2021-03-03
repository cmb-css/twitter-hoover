import glob
import gzip
import json
import os
from hoover.users import get_user_ids


def _simple(tweet):
    text = tweet['full_text'] if 'full_text' in tweet else tweet['text']
    data = {
        'id': tweet['id_str'],
        'text': text,
        'created_at': tweet['created_at'],
        'user': tweet['user']['screen_name'],
        'user_id': tweet['user']['id'],
        'followers_count': tweet['user']['followers_count'],
        'friends_count': tweet['user']['friends_count'],
        'is_quote': tweet['is_quote_status'],
        'urls': [],
        'quotes': []}
    if 'entities' in tweet:
        if 'urls' in tweet['entities']:
            for url_entity in tweet['entities']['urls']:
                data['urls'].append(url_entity['expanded_url'])
    return data


class ExtractQuotes(object):
    def __init__(self, infile, indir, outfile):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

        self.n_trees = 0
        self.n_quotes = 0

        self.tweets = {}

    def _filter(self, json_str):
        return 'quoted_status' in json_str

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        file_names = []
        for i in range(1, 13):
            file_names += glob.glob(os.path.join(
                self._user_path(user_id), '2020-{:02}.json.gz'.format(i)))
        return file_names

    def _process_file(self, infile):
        # print('infile: {}'.format(infile))
        with gzip.open(infile, 'rt') as f:
            for line in f:
                if self._filter(line):
                    try:
                        tweet = json.loads(line)
                        if 'quoted_status' in tweet:
                            tweet_id = tweet['id_str']
                            if tweet_id not in self.tweets:
                                qs = tweet['quoted_status']
                                quid = qs['user']['id']
                                if quid in self.user_ids:
                                    self.n_quotes += 1
                                    parent = qs
                                    parent_id = parent['id_str']

                                    # add parent if it does not exist yet
                                    if parent_id not in self.tweets:
                                        sparent = _simple(parent)
                                        self.tweets[parent_id] = sparent
                                        if not sparent['is_quote']:
                                            self.n_trees += 1

                                    stweet = _simple(tweet)
                                    self.tweets[tweet_id] = stweet
                                    self.tweets[parent_id]['quotes'].append(
                                        stweet)
                    except json.decoder.JSONDecodeError:
                        pass

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                self._process_file(infile)
            print('trees: {}; quotes: {}'.format(self.n_trees, self.n_quotes))

        # write trees
        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for tid in self.tweets:
                tweet = self.tweets[tid]
                if not tweet['is_quote']:
                    f.write('{}\n'.format(
                        json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    tr = ExtractQuotes(
        'eu-elections-userids.csv', 'timelines', 'quotes-2020.json')
    tr.run()
