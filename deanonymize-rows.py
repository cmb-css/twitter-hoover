import argparse

from tqdm import tqdm

from hoover.anon.decrypt_indiv import deanonymize


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))

    with open(infile, 'rt') as f:
        rows = [line.strip() for line in f]

    with open(outfile, 'wt') as f:
        for row in tqdm(rows):
            _row = deanonymize(row, args.anon_db_folder_path)
            uid = _row.split(',')[0]
            if uid != 'user_id':
                f.write(f'{uid}\n')
