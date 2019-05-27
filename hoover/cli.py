import argparse
from hoover.auth import auth_app
from hoover.stream import read_stream
from hoover.simple import simplify


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument('command', type=str, help='command to execute')
    parser.add_argument('--infile', type=str,
                        help='input file', default=None)
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--errfile', type=str,
                        help='error file', default='error.log')
    parser.add_argument('--key', type=str, help='key & secret file',
                        default='key-and-secret.txt')
    parser.add_argument('--auth', type=str, help='auth file',
                        default='auth.txt')
    parser.add_argument('--keywords', type=str, help='hastags/keywords file',
                        default=None)

    args = parser.parse_args()

    if args.keywords:
        print('keywords file: {}'.format(args.keywords))
    if args.outfile:
        print('output file: {}'.format(args.outfile))

    if args.command == 'auth':
        auth_app(args.key, args.auth)
    elif args.command == 'stream':
        read_stream(args.key, args.auth, args.keywords,
                    args.outfile, args.errfile)
    elif args.command == 'simplify':
        simplify(args.infile)
    else:
        print('Unknown command: {}'.format(args.command))
