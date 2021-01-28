import glob
import gzip
import json
import os
from hoover.users import get_user_ids


class ExtractUrtweets(object):
    def __init__(self, infile, indir, outfile, hashtags):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

        self.n_matches = 0
        self.urtweets = []

    def _filter(self, json_str):
        # HACK
        return '#remaniement' in json_str.lower()

    def _simple(self, tweet):
        text = tweet['full_text'] if 'full_text' in tweet else tweet['text']
        return {
            'id': tweet['id_str'],
            'text': text,
            'created_at': tweet['created_at'],
            'user': tweet['user']['screen_name'],
            'user_id': tweet['user']['id'],
            'followers_count': tweet['user']['followers_count'],
            'friends_count': tweet['user']['friends_count']}

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # HACK!!! (DATE)
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '2020-06.json.gz'))
        file_names += glob.glob(
            os.path.join(self._user_path(user_id), '2020-07.json.gz'))
        return file_names

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        if self._filter(line):
                            self.n_matches += 1
                            tweet = json.loads(line)
                            if ('retweeted_status' not in tweet and
                                    'quoted_status' not in tweet):
                                self.urtweets.append(self._simple(tweet))

                print('matches: {}; ur-tweets: {}'.format(
                    self.n_matches, len(self.urtweets)))

        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for urtweet in self.urtweets:
                f.write('{}\n'.format(json.dumps(urtweet, ensure_ascii=False)))


if __name__ == '__main__':
    tr = ExtractUrtweets('retweet-userids.csv', 'timelines', 'ur-tweets.json')
    tr.run()
