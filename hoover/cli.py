import argparse
from hoover.auth import auth_app


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument('command', type=str, help='command to execute')
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--key', type=str, help='key & secret file',
                        default='key-and-secret.txt')
    parser.add_argument('--auth', type=str, help='auth file',
                        default='auth.txt')

    args = parser.parse_args()

    if args.outfile:
        print('output file: {}'.format(args.outfile))

    if args.command == 'auth':
        auth_app(args.key, args.auth)
    else:
        print('Unknown command: {}'.format(args.command))
