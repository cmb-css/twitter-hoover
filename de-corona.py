import glob
import gzip
import os


YEAR_MONTHS = ['2019-11', '2019-12', '2020-01', '2020-02', '2020-03',
               '2020-04', '2020-05', '2020-06', '2020-07']


if __name__ == '__main__':
    dir_names = glob.glob('timelines/*')
    n = len(dir_names)

    with gzip.open('corona-de.json.gz', 'wt') as output_file:
        for i, dir_name in enumerate(dir_names):
            print('[{}/{}] {}'.format(i, n, dir_name))
            user_id = os.path.basename(dir_name)
            for ym in YEAR_MONTHS:
                file_name = 'timelines/{}/{}.json.gz'.format(user_id, ym)
                try:
                    with gzip.open(file_name, 'rt') as f:
                        for line in f.readlines():
                            lline = line.lower()
                            if ('"lang": "de"' in lline and
                                ('corona' in lline or 'covid' in lline)):
                                output_file.write(line)
            except FileNotFoundError:
                pass
    print('done.')
