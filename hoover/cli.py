import argparse
import time
import calendar
from hoover.auth import auth_app
from hoover.stream import read_stream
from hoover.timelines import retrieve_timelines
from hoover.users import (retrieve_friends, retrieve_followers,
                          retrieve_friend_ids, retrieve_follower_ids)
from hoover.simple import simplify
from hoover.csv import to_csv
from hoover.youtube import extract_videos
from hoover.hydrate import hydrate_file


def ddmmyy2utc(s):
    return calendar.timegm(time.strptime(s, '%d.%m.%Y'))


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument('command', type=str, help='command to execute')
    parser.add_argument('--infile', type=str,
                        help='input file', default=None)
    parser.add_argument('--outfile', type=str,
                        help='output file', default=None)
    parser.add_argument('--indir', type=str,
                        help='input directory', default=None)
    parser.add_argument('--outdir', type=str,
                        help='output directory', default=None)
    parser.add_argument('--errfile', type=str,
                        help='error file', default='error.log')
    parser.add_argument('--key', type=str, help='key & secret file',
                        default='key-and-secret.txt')
    parser.add_argument('--auth', type=str, help='auth file',
                        default='auth.txt')
    parser.add_argument('--mindate', type=str, help='earliest date for tweets',
                        default=None)
    parser.add_argument('--noretweets', help='do not retrieve retweets',
                        action='store_true')
    parser.add_argument('--user', type=str, help='user screen name or id',
                        default=None)
    parser.add_argument('--type', type=str, help='type', default=None)

    args = parser.parse_args()

    if args.infile:
        print('input file: {}'.format(args.infile))
    if args.outfile:
        print('output file: {}'.format(args.outfile))
    if args.indir:
        print('input directory: {}'.format(args.indir))
    if args.outdir:
        print('output directory: {}'.format(args.outdir))

    min_utc = None
    if args.mindate:
        min_utc = ddmmyy2utc(args.mindate)
        print('minimum date: {}'.format(args.mindate))

    if args.noretweets:
        print('not retrieving retweets')

    if args.command == 'auth':
        auth_app(args.key, args.auth)
    elif args.command == 'stream':
        read_stream(args.key, args.auth, args.infile,
                    args.outfile, args.errfile)
    elif args.command == 'timelines':
        retrieve_timelines(args.key, args.auth, args.infile, args.user,
                           args.outdir, args.errfile, min_utc,
                           not args.noretweets)
    elif args.command == 'friends':
        retrieve_friends(args.key, args.auth, args.user, args.outfile,
                         args.infile, args.outdir)
    elif args.command == 'followers':
        retrieve_followers(args.key, args.auth, args.user, args.outfile,
                           args.infile, args.outdir)
    elif args.command == 'friend_ids':
        retrieve_friend_ids(args.key, args.auth, args.user, args.outfile,
                            args.infile, args.outdir)
    elif args.command == 'follower_ids':
        retrieve_follower_ids(args.key, args.auth, args.user, args.outfile,
                              args.infile, args.outdir)
    elif args.command == 'simplify':
        simplify(args.infile)
    elif args.command == 'csv':
        to_csv(args.infile, args.outfile, args.indir, args.outdir, args.type)
    elif args.command == 'youtube':
        extract_videos(args.infile)
    elif args.command == 'hydrate':
        hydrate_file(args.key, args.auth, args.infile, args.outfile,
                     args.errfile)
    else:
        print('Unknown command: {}'.format(args.command))
