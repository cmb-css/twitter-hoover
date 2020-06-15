import glob
import gzip
import os


YEAR_MONTH = '2020-03'


if __name__ == '__main__':
    dir_names = glob.glob('timelines/*')
    n = len(dir_names)

    count = 0

    for i, dir_name in enumerate(dir_names):
        print('[{}/{}] {}'.format(i, n, dir_name))
        user_id = os.path.basename(dir_name)
        file_name = 'timelines/{}/{}.json.gz'.format(user_id, YEAR_MONTH)

        try:
            with gzip.open(file_name, 'r') as f:
                for line in f.readlines():
                    count += 1
        except FileNotFoundError:
            pass

    print('total tweets: {}'.format(count))
