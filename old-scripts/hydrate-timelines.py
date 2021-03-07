import os
import glob
import gzip
from hoover.users import get_user_ids
from hoover.hydrate import hydrate_file


def new_file_name(file_name):
    return '{}-hydrated.json.gz'.format(file_name.split('.')[0])


class HydrateTimelines(object):
    def __init__(self, infile, outdir):
        self.user_ids = get_user_ids(infile)
        self.outdir = outdir

    def _user_path(self, user_id):
        return os.path.join(self.outdir, str(user_id))

    def _user_files(self, user_id):
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '*.json.gz'))
        return file_names

    def _remove_hydrated(self, user_id):
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '*hydrated*.json.gz'))
        for file in file_names:
            # TEMPORARY HACK!!!
            if '2020-07-hydrated.json.gz' in file:
                os.remove(file)

    def hydrate(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            # self._remove_hydrated(user_id)
            for infile in self._user_files(user_id):
                # TEMPORARY HACK!!!
                if '2020-07.json.gz' in infile:
                    outfile = new_file_name(infile)
                    try:
                        os.remove(outfile)
                    except OSError:
                        pass
                    print('infile: {}\noutfile: {}'.format(infile, outfile))
                    hydrate_file('key-and-secret.txt', 'auth.txt',
                                 infile, outfile, 'error.log')


if __name__ == '__main__':
    htl = HydrateTimelines('eu-elections-userids.csv', 'timelines')
    htl.hydrate()
