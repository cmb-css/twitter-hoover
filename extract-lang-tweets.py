import argparse
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
                        gzip.open(self.outfile, 'at') as of:
                    for line in f:
                        if self.lang_str in line:
                            of.write('{}\n'.format(line))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--infile', type=str,
                        help='input file', default='eu-elections-userids.csv')
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--indir', type=str,
                        help='input directory', default='timelines')
    parser.add_argument('--outdir', type=str,
                        help='output directory', default=None)
    parser.add_argument('--lang', type=str,
                        help='language', default=None)
    parser.add_argument('--month', type=str, help='month',
                        default=None)

    args = parser.parse_args()

    elt = ExtractLangTweets(
        args.infile,
        args.indir,
        args.outfile,
        args.lang,
        args.month)
    elt.run()
