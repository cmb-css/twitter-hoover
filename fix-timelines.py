import os
import glob
import gzip
from hoover.users import get_user_ids


def json_split(json_str):
    if len(json_str.split('}{')) == 1:
        return [json_str.strip()]
    parts = []
    depth = 0
    part = ''
    for i, c in enumerate(json_str.strip()):
        part += c
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
        if depth == 0:
            parts.append(part)
            part = ''
    return parts


class FixTimeline(object):
    def __init__(self, infile, outdir):
        self.user_ids = get_user_ids(infile)
        self.outdir = outdir

    def _user_path(self, user_id):
        return os.path.join(self.outdir, str(user_id))

    def _cur_file(self, user_id):
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '*.json.gz'))
        max_date_month = 0
        latest_file = None
        for file_name in file_names:
            base = os.path.basename(file_name)
            base = base.split('.')[0]
            date_month = int(base.replace('-', ''))
            if date_month > max_date_month:
                max_date_month = date_month
                latest_file = file_name
        print('latest_file: {}'.format(latest_file))
        return latest_file

    def fix(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            cur_file = self._cur_file(user_id)
            if cur_file:
                with gzip.open(cur_file, 'rt') as f:
                    for line in f:
                        parts = json_split(line)
                        if len(parts) > 1:
                            print(parts)


if __name__ == '__main__':
    ftl = FixTimeline('eu-elections-userids.csv', 'timelines')
    ftl.fix()
