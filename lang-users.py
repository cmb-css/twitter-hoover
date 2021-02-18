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
            done = False
            for infile in self._user_files(user_id):
                if done:
                    break
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        user_tweets += 1
                        try:
                            tweet = json.loads(line)
                            lang_counter[tweet['lang']] += 1
                        except json.decoder.JSONDecodeError:
                            pass

            if user_tweets > 0:
                percents = list((lang, count / user_tweets)
                                for lang, count in lang_counter.most_common())
                if list[0][0] == self.lang and list[0][1] >= 15.0:
                    self.lang_users.append(user_id)
                print(' | '.join(['{} {:.0f}%'.format(lang, percent)
                      for lang, percent in percents]))
            print('users: {}; lang-users: {}'.format(
                self.n_users, len(self.lang_users)))

        with open(self.outfile, 'wt') as f:
            for uid in self.lang_users:
                f.write('{}\n'.format(json.dumps(uid)))


if __name__ == '__main__':
    tr = LangUsers(
        'eu-elections-userids.csv', 'timelines', 'fr-userids.csv', 'fr')
    tr.run()
