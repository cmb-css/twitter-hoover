import os
import glob
import gzip
import json
from hoover.users import get_user_ids


class TestRetweets(object):
    def __init__(self, infile, indir):
        self.user_ids = get_user_ids(infile)
        self.indir = indir

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # TEMPORARY HACK!!! (DATE)
        file_names = glob.glob(
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
                        tweet = json.loads(line)
                        print(tweet['in_reply_to_user_id'])


if __name__ == '__main__':
    tr = TestRetweets('eu-elections-userids.csv', 'timelines')
    tr.run()
