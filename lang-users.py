import os
import glob
import gzip
import json
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
            user_tweets = 0
            lang_tweets = 0
            done = False
            for infile in self._user_files(user_id):
                if done:
                    break
                self.n_users += 1
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        user_tweets += 1
                        tweet = json.loads(line)
                        if tweet['lang'] == self.lang:
                            lang_tweets += 1
                        # number of language tweets to consider language user
                        if lang_tweets >= 5:
                            self.lang_users.append(user_id)
                            done = True
                            break
                        # number of tweets until giving up
                        if user_tweets >= 1000 and lang_tweets == 0:
                            done = True
                            break

            print('users: {}; lang-users: {}'.format(
                self.n_users, len(self.lang_users)))

        with open(self.outfile, 'wt') as f:
            for uid in self.lang_users:
                f.write('{}\n'.format(json.dumps(uid)))


if __name__ == '__main__':
    tr = LangUsers(
        'eu-elections-userids.csv', 'timelines', 'fr-userids.csv', 'fr')
    tr.run()
