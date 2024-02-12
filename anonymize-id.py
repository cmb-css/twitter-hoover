import argparse
import pickle
import os

from hoover.anon.anonymize_v1 import anonymize_raw


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=str, help='id to anonymize', default=None)
    # 'UID' for user ID, 'USN' for user name, 'TID' for tweet ID and 'TURL' for tweet URL.
    parser.add_argument('--data_type', type=str, help='data type, can be UID, USN, TID or TURL', default='UID')
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    print(f'user id: {args.id}')
    print(f'data type: {args.data_type}')

    print('result: {}'.format(anonymize_raw(args.id, args.data_type, anon_dict)))
