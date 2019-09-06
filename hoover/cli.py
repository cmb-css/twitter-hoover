import argparse
import time
import calendar
from hoover.auth import auth_app
from hoover.stream import read_stream
from hoover.timelines import retrieve_timelines
from hoover.simple import simplify
from hoover.youtube import extract_videos


def ddmmyy2utc(s):
    return calendar.timegm(time.strptime(s, '%d.%m.%Y'))


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument('command', type=str, help='command to execute')
    parser.add_argument('--infile', type=str,
                        help='input file', default=None)
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--outdir', type=str,
                        help='output directory', default=None)
    parser.add_argument('--errfile', type=str,
                        help='error file', default='error.log')
    parser.add_argument('--key', type=str, help='key & secret file',
                        default='key-and-secret.txt')
    parser.add_argument('--auth', type=str, help='auth file',
                        default='auth.txt')
    parser.add_argument('--keywords', type=str, help='hastags/keywords file',
                        default=None)
    parser.add_argument('--mindate', type=str, help='earliest date for tweets',
                        default=None)

    args = parser.parse_args()

    if args.infile:
        print('input file: {}'.format(args.infile))
    if args.keywords:
        print('keywords file: {}'.format(args.keywords))
    if args.outfile:
        print('output file: {}'.format(args.outfile))
    if args.outdir:
        print('output directory: {}'.format(args.outdir))

    min_utc = None
    if args.mindate:
        min_utc = ddmmyy2utc(args.mindate)
        print('minimum date: {}'.format(args.mindate))

    if args.command == 'auth':
        auth_app(args.key, args.auth)
    elif args.command == 'stream':
        read_stream(args.key, args.auth, args.keywords,
                    args.outfile, args.errfile)
    elif args.command == 'timelines':
        retrieve_timelines(args.key, args.auth, args.infile,
                           args.outdir, args.errfile, min_utc)
    elif args.command == 'simplify':
        simplify(args.infile)
    elif args.command == 'youtube':
        extract_videos(args.infile)
    else:
        print('Unknown command: {}'.format(args.command))
