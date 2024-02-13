import argparse
import pickle
import os

from tqdm import tqdm

from hoover.anon.anonymize_v1 import anonymize_raw


if __name__ == '__main__':
    # Possible values are 'UID' for user ID, 'USN' for user name, 'TID' for tweet ID and 'TURL' for tweet URL.
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='input file', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    # Possible values are 'UID' for user ID, 'USN' for user name, 'TID' for tweet ID and 'TURL' for tweet URL.
    parser.add_argument('--data_type', type=str, help='output file', default=None)
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    data_type = args.data_type

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('data_type: {}'.format(data_type))

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    with open(infile, 'rt') as f:
        ids = [line.strip() for line in f]

    with open(outfile, 'wt') as f:
        for _id in tqdm(ids):
            anon_id = anonymize_raw(_id, data_type, anon_dict)
            f.write(f'{anon_id}\n')
