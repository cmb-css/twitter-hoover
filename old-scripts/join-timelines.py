import glob
import gzip
import os


YEAR_MONTH = '2020-03'


if __name__ == '__main__':
    dir_names = glob.glob('timelines/*')
    n = len(dir_names)

    with gzip.open('{}.json.gz'.format(YEAR_MONTH), 'wt') as output_file:
        for i, dir_name in enumerate(dir_names):
            print('[{}/{}] {}'.format(i, n, dir_name))
            user_id = os.path.basename(dir_name)
            file_name = 'timelines/{}/{}.json.gz'.format(user_id, YEAR_MONTH)
            try:
                with gzip.open(file_name, 'rt') as f:
                    for line in f.readlines():
                        if '"lang": "de"' in line:
                            output_file.write(line)
            except FileNotFoundError:
                pass
    print('done.')
