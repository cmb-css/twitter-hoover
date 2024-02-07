import argparse
import glob
import pickle
import os

from hoover.anon.anonymize_v1 import anonymize_raw


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--indir', type=str, help='input directory', default=None)
    parser.add_argument('--outdir', type=str, help='output directory', default=None)
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    indir = args.indir
    outdir = args.outdir

    print('indir: {}'.format(indir))
    print('outdir: {}'.format(outdir))

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    file_paths = glob.glob(os.path.join(indir, '*.csv'))

    n = 0
    for file_path in file_paths:
        n += 1
        print(f'processing {file_path}')
        with open(file_path, 'rt') as f:
            follower_ids = [line.strip() for line in f]

        user_id = file_path.split('/')[-1].split('-')[0]
        anon_user_id = anonymize_raw(user_id, 'UID', anon_dict)

        out_file_path = os.path.join(outdir, f'{anon_user_id}-followers_ids.csv')
        with open(out_file_path, 'wt') as f:
            for follower_id in follower_ids:
                anon_follower_id = anonymize_raw(follower_id, 'UID', anon_dict)
                f.write(f'{anon_follower_id}\n')

    print(f'{n} files anonymized.')
