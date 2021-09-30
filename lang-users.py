import argparse
import os
import glob
import gzip
import json
from collections import Counter
from hoover.users import get_user_ids


class LangUsers(object):
    def __init__(self, infile, indir, outfile, lang):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.lang = lang

        self.n_users = 0
        self.lang_users = []

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '*.json.gz'))
        return list(file_name for file_name in file_names
                    if 'hydrated' not in file_name)

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            self.n_users += 1
            user_tweets = 0
            lang_counter = Counter()
            screen_name = '?'
            done = False
            for infile in self._user_files(user_id):
                if done:
                    break
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        user_tweets += 1
                        if user_tweets >= 1000:
                            done = True
                            break
                        try:
                            tweet = json.loads(line)
                            lang_counter[tweet['lang']] += 1
                            if (screen_name == '?' and
                                    tweet['user']['id'] == user_id):
                                screen_name = tweet['user']['screen_name']

                        except json.decoder.JSONDecodeError:
                            pass

            if user_tweets > 0:
                percents = list((lang, (count / user_tweets) * 100.0)
                                for lang, count in lang_counter.most_common())
                if percents[0][0] == self.lang and percents[0][1] >= 15.0:
                    self.lang_users.append((user_id, screen_name))
                print(' | '.join(['{} {:.0f}%'.format(lang, percent)
                      for lang, percent in percents]))
            print('users: {}; lang-users: {}'.format(
                self.n_users, len(self.lang_users)))

        with open(self.outfile, 'wt') as f:
            f.write('user_id,screen_name,profile\n')
            for user_id, screen_name in self.lang_users:
                f.write('{},{},https://twitter.com/{}\n'.format(
                    user_id, screen_name, screen_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lang', type=str, help='language')
    parser.add_argument('--outfile', type=str, help='output file')
    args = parser.parse_args()

    lang = args.lang
    outfile = args.outfile

    print('lang: {}'.format(lang))
    print('outfile: {}'.format(outfile))

    tr = LangUsers('eu-elections-userids.csv', 'timelines', outfile, lang)
    tr.run()
