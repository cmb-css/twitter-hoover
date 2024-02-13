import argparse

from hoover.anon.decrypt_indiv import deanonymize


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=str, help='id to anonymize', default=None)
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    print(f'user id: {args.id}')
    print('result: {}'.format(deanonymize(args.id, args.anon_db_folder_path)))
