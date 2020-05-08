import os
import json
import glob
from datetime import datetime
import gzip
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

        with open(file_name, 'r') as f:
            last_month_year = None
            of = None
            for line in f:
                data = json.loads(line)
                ts = str2utc(data['created_at'])
                month_year = datetime.utcfromtimestamp(ts).strftime('%Y-%m')
                if last_month_year != month_year:
                    if of:
                        of.close()
                    outfile = '{}/{}.json.gz'.format(dirpath, month_year)
                    of = gzip.open(outfile, 'at')
                    last_month_year = month_year
                of.write('{}\n'.format(json.dumps(data)))
            of.close()
