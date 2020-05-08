import os
import glob
from datetime import datetime
import gzip
from collections import defaultdict
from hoover.snowflake import str2utc


if __name__ == '__main__':
    file_names = glob.glob('timelines/*.json')
    n = len(file_names)
    i = 0
    for i, file_name in enumerate(file_names):
        print('[{}/{}] {}'.format(i, n, file_name))
        base = os.path.basename(file_name)
        base = base.split('.')[0]
        dirpath = 'timelines-new/{}'.format(base)
        os.mkdir(dirpath)

        tweets = defaultdict(list)
        with open(file_name, 'r') as f:
            for line in f.readlines():
                ts = str2utc(line[16:46])
                month_year = datetime.utcfromtimestamp(ts).strftime('%Y-%m')
                tweets[month_year].append(line)
            for month_year in tweets:
                outfile = '{}/{}.json.gz'.format(dirpath, month_year)
                with gzip.open(outfile, 'wt') as of:
                    of.write('\n'.join(tweets[month_year]))
