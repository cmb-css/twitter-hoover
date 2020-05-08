import os
from subprocess import check_call
import glob
from collections import defaultdict


def date_str2month_year(date_str):
    month_str = date_str[4:7]
    year_str = date_str[-4:]
    month = ''
    if month_str == 'Jan':
        month = '01'
    elif month_str == 'Feb':
        month = '02'
    elif month_str == 'Mar':
        month = '03'
    elif month_str == 'Apr':
        month = '04'
    elif month_str == 'May':
        month = '05'
    elif month_str == 'Jun':
        month = '06'
    elif month_str == 'Jul':
        month = '07'
    elif month_str == 'Aug':
        month = '08'
    elif month_str == 'Sep':
        month = '09'
    elif month_str == 'Oct':
        month = '10'
    elif month_str == 'Nov':
        month = '11'
    elif month_str == 'Dec':
        month = '12'
    else:
        raise RuntimeError('Unkown month: {}'.format(month_str))
    return '{}-{}'.format(year_str, month)


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
                month_year = date_str2month_year(line[16:46])
                tweets[month_year].append(line)
            for month_year in tweets:
                outfile = '{}/{}.json'.format(dirpath, month_year)
                with open(outfile, 'wt') as of:
                    of.write(''.join(tweets[month_year]))
                check_call(['gzip', outfile])
        check_call(['rm', file_name])
