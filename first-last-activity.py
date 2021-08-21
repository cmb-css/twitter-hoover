import os

from os import listdir
from hoover.users import get_user_ids


def get_tweet_ids(tweet_ids_file):
    tweet_ids = set()
    with open(tweet_ids_file, 'rt') as f:
        for line in f:
            tid = line.strip()
            if len(tid) > 0:
                tweet_ids.add(tid)
    print('{} tweet ids read'.format(len(tweet_ids)))
    return tweet_ids


class FirstLastActivity(object):
    def __init__(self, infile, indir, outfile):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def min_max_dates(self, user_id):
        min_date = '9999-99'
        max_date = '0000-00'
        for filename in listdir(self._user_path(user_id)):
            if filename[-8:] == '.json.gz':
                date = filename[:7]
                if date < min_date:
                    min_date = date
                if date > max_date:
                    max_date = date

        return min_date, max_date

    def run(self):
        with open(self.outfile, 'wt') as f:
            f.write('user_id,min_date,max_date\n')

        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            min_date, max_date = self.min_max_dates(user_id)
            print('{} -> {}'.format(min_date, max_date))

            with open(self.outfile, 'at') as f:
                f.write('{},{},{}\n'.format(user_id, min_date, max_date))


if __name__ == '__main__':
    fla = FirstLastActivity('eu-elections-userids.csv', 'timelines',
                            'first-last-activity.csv')
    fla.run()
