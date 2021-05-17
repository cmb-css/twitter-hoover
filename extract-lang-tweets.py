import os
import glob
import gzip
from hoover.users import get_user_ids


class ExtractLangTweets(object):
    def __init__(self, infile, indir, outfile, lang, month):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.lang = lang
        self.month = month

        self.lang_str = '"lang": "{}"'.format(self.lang)

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        file_names = glob.glob(
            os.path.join(
                self._user_path(user_id),
                '{}-hydrated.json.gz'.format(self.month)))
        if len(file_names) > 0:
            return file_names
        return glob.glob(
            os.path.join(
                self._user_path(user_id), '{}.json.gz'.format(self.month)))

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                with gzip.open(infile, 'rt') as f,\
                        gzip.open(self.outfile, 'wt') as of:
                    for line in f:
                        if self.lang_str in line:
                            self.tweets += 1
                            of.write('{}\n'.format(line))


if __name__ == '__main__':
    tr = ExtractLangTweets(
        'eu-elections-userids.csv',
        'timelines',
        'fr-2019-12.json.gz',
        'fr',
        '2019-12')
    tr.run()
