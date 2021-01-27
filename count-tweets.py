import gzip
from pathlib import Path


if __name__ == '__main__':
    p = Path('timelines').glob('*')
    dirs = [x for x in p if x.is_dir()]

    count = 0
    for d in dirs:
        print('directory: {}'.format(d))
        p = Path(d).glob('*.json.gz')
        files = [x for x in p if x.is_file() and 'hydated' not in x]
        for file in files:
            print('file: {}'.format(file))
            with gzip.open(file, 'rt') as f:
                for line in f:
                    count += 1
            print('count: {}'.format(count))
