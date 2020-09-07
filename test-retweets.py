import os
import glob
import gzip
from hoover.users import get_user_ids
from hoover.hydrate import hydrate_file


class TestRetweets(object):
    def __init__(self, infile, indir):
        self.user_ids = get_user_ids(infile)
        self.indir = indir

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # file_names = glob.glob(
        #     os.path.join(self._user_path(user_id), '*.json.gz'))
        # return file_names
        return ['2020-07.json.gz']

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                # TODO: temporary hack
                if '-07.json.gz' not in infile:
                    outfile = new_file_name(infile)
                    try:
                        os.remove(outfile)
                    except OSError:
                        pass
                    print('infile: {}\noutfile: {}'.format(infile, outfile))
                    hydrate_file('key-and-secret.txt', 'auth.txt',
                                 infile, outfile, 'error.log')


if __name__ == '__main__':
    htl = TestRetweets('eu-elections-userids.csv', 'timelines')
    htl.run()
