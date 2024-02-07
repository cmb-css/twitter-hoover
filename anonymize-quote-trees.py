import argparse
import json
import pickle
import os

from hoover.anon.anonymize_v1 import anonymize_raw, anonymize_text


# 'UID' for user ID, 'USN' for user name, 'TID' for tweet ID and 'TURL' for tweet URL.
def anonymize_tree(tree, anon_dict):
    tree['id'] = anonymize_raw(str(tree['id']), 'TID', anon_dict)
    tree['text'] = anonymize_text(tree['text'], anon_dict)
    tree['user'] = anonymize_raw(tree['user'], 'USN', anon_dict)
    tree['user_id'] = anonymize_raw(str(tree['user_id']), 'UID', anon_dict)
    tree['urls'] = [anonymize_raw(url, 'TURL', anon_dict) for url in tree['urls']]
    if tree['in_reply_to_user']:
        tree['in_reply_to_user'] = anonymize_raw(tree['in_reply_to_user'], 'USN', anon_dict)
    if tree['in_reply_to_user_id']:
        tree['in_reply_to_user_id'] = anonymize_raw(str(tree['in_reply_to_user_id']), 'UID', anon_dict)
    if tree['in_reply_to_status_id']:
        tree['in_reply_to_status_id'] = anonymize_raw(str(tree['in_reply_to_status_id']), 'TID', anon_dict)
    tree['quotes'] = [anonymize_tree(_tree, anon_dict) for _tree in tree['quotes']]
    tree['quote_ids'] = [anonymize_raw(str(quote_id), 'TID', anon_dict) for quote_id in tree['quote_ids']]
    if 'retweeters' in tree:
        tree['retweeters'] = [anonymize_raw(str(user_id), 'UID', anon_dict) for user_id in tree['retweeters']]


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

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    n = 0
    with open(args.infile, 'rt') as in_f, open(args.outfile, 'wt') as out_f:
        for line in in_f:
            n += 1
            tree = json.loads(line)
            anonymize_tree(tree, anon_dict)
            tree_str = json.dumps(tree)
            out_f.write(f'{tree_str}\n')

    print(f'{n} trees anonymized.')
