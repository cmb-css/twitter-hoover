import argparse
import os
import json
import pickle

from v2.search_v2 import anonymize_v2


def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, help="Path to the file to be encrypted.")
    parser.add_argument("--output_path", type=str, help="Path to the encrypted file.")
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    # parser.add_argument("--compressed", type=str, help="Whether the files are compressed or not.")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args_from_command_line()

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    n = 0
    with open(args.input_path, 'rt') as in_f, open(args.output_path, 'wt') as out_f:
        for line in in_f:
            n += 1
            tweet_dict = json.loads(line)
            tweet_dict = anonymize_v2(response=tweet_dict, anon_dict=anon_dict)
            tweet_str = json.dumps(tweet_dict)
            out_f.write(f'{tweet_str}\n')

    print(f'{n} tweets anonymized.')
