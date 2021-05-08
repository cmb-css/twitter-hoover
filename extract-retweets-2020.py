import argparse
import glob
import gzip
import json
import os
from hoover.users import get_user_ids


def get_tweet_ids(tweet_ids_file):
    tweet_ids = set()
    with open(tweet_ids_file, 'rt') as f:
        for line in f:
            tid = line.strip()
            if len(tid) > 0:
                tweet_ids.add(tid)


class ExtractRetweets(object):
    def __init__(self, tweet_ids_file, infile, indir, outfile, month):
        self.tweet_ids = get_tweet_ids(tweet_ids_file)
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.month = month

        self.retweet_ids = set()
        self.retweets = {}

    def _filter(self, json_str):
        return 'retweeted_status' in json_str

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        file_names = glob.glob(os.path.join(
            self._user_path(user_id),
            '2020-{:02}-hydrated.json.gz'.format(self.month)))
        file_names += glob.glob(os.path.join(
            self._user_path(user_id), '2020-{:02}.json.gz'.format(
                self.month)))
        return file_names

    def _process_file(self, infile):
        # print('infile: {}'.format(infile))
        with gzip.open(infile, 'rt') as f:
            for line in f:
                if self._filter(line):
                    try:
                        tweet = json.loads(line)
                        if 'retweeted_status' in tweet:
                            tweet_id = tweet['retweeted_status']['id_str']
                            if tweet_id in self.tweet_ids:
                                retweet_id = tweet['id_str']
                                if retweet_id not in self.retweet_ids:
                                    self.retweet_ids.add(retweet_id)
                                    user_id = tweet['user']['id_str']
                                    if tweet_id not in self.retweets[]:
                                        self.retweets[tweet_id] = []
                                    self.retweets[tweet_id].append()
                    except json.decoder.JSONDecodeError:
                        pass

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                self._process_file(infile)
            print('retweets: {}'.format(len(self.retweet_ids)))

        # write retweets
        with open(self.outfile, 'wt', encoding='utf-8') as f:
            f.write('{}\n'.format(json.dumps(self.retweets)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--month', type=int,
                        help='month', default=None)
    args = parser.parse_args()

    outfile = args.outfile
    month = args.month

    print('outfile: {}'.format(outfile))
    print('month: {:02}'.format(month))

    tr = ExtractRetweets(
        'quotes-2020-tweet-ids.csv', 'eu-elections-userids.csv', 'timelines',
        outfile, month)
    tr.run()
