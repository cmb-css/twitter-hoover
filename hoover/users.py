import csv
import os.path
from twython import TwythonError
from hoover.snowflake import str2utc
from hoover.auth import twython_from_key_and_auth
from hoover.rate_control import RateControl


USER_FIELDS = ('id',
               'screen_name',
               'name',
               'location',
               'protected',
               'verified',
               'followers_count',
               'friends_count',
               'listed_count',
               'favourites_count',
               'statuses_count',
               'created_at',
               'created_ts')


def get_user_ids(file):
    user_ids = []
    with open(file) as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            try:
                user_id = int(row[0])
                user_ids.append(user_id)
            except ValueError:
                pass
    return user_ids


class Users(RateControl):
    def __init__(self, key_file, auth_file):
        super().__init__(rate_limit=14)
        self.twitter = twython_from_key_and_auth(key_file, auth_file)

    def screen_name2id(self, screen_name):
        self.pre_request(verbose=True)
        response = self.twitter.lookup_user(screen_name=screen_name)
        return int(response[0]['id'])

    def user2id(self, user):
        try:
            return int(user)
        except ValueError:
            return self.screen_name2id(user)

    def retrieve(self, user, entity_type, outfile):
        try:
            user_id = self.user2id(user)
            users = []
            cursor = -1
            while cursor != 0:
                self.pre_request(verbose=True)
                if entity_type == 'friends':
                    response = self.twitter.get_friends_list(user_id=user_id,
                                                             cursor=cursor,
                                                             count=200)
                elif entity_type == 'followers':
                    response = self.twitter.get_followers_list(user_id=user_id,
                                                               cursor=cursor,
                                                               count=200)
                else:
                    raise RuntimeError(
                        'Unknown entity type: "{}".'.format(entity_type))
                cursor = response['next_cursor']
                users += response['users']

            with open(outfile, 'w') as outfile:
                csvwriter = csv.writer(outfile)
                csvwriter.writerow(USER_FIELDS)
                for user in users:
                    user['created_ts'] = str2utc(user['created_at'])
                    csvwriter.writerow([user[field] for field in USER_FIELDS])
            print('{} {} found.'.format(len(users), entity_type))
        except TwythonError as e:
            print('ERROR: {}'.format(e))

    def retrieve_ids(self, user, entity_type, outfile):
        try:
            user_id = self.user2id(user)
            user_ids = []
            cursor = -1
            while cursor != 0:
                self.pre_request(verbose=True)
                if entity_type == 'friends_ids':
                    response = self.twitter.get_friends_ids(user_id=user_id,
                                                            cursor=cursor,
                                                            count=5000)
                elif entity_type == 'followers_ids':
                    response = self.twitter.get_followers_ids(user_id=user_id,
                                                              cursor=cursor,
                                                              count=5000)
                else:
                    raise RuntimeError(
                        'Unknown entity type: "{}".'.format(entity_type))
                cursor = response['next_cursor']
                user_ids += response['ids']

            with open(outfile, 'w') as outfile:
                csvwriter = csv.writer(outfile)
                for user_id in user_ids:
                    csvwriter.writerow([user_id])
            print('{} {} found.'.format(len(user_ids), entity_type))
        except TwythonError as e:
            print('ERROR: {}'.format(e))


def retrieve(entity_type, key_file, auth_file,
             user, outfile, infile, outdir):
    if user:
        if infile:
            raise RuntimeError(
                'Only one of --user and --infile can be provided.')
        if not outfile:
            raise RuntimeError('--outfile must be provided.')
        Users(key_file, auth_file).retrieve(user, entity_type, outfile)
    elif infile:
        if user:
            raise RuntimeError(
                'Only one of --user and --infile can be provided.')
        if not outdir:
            raise RuntimeError('--outdir must be provided.')
        users = Users(key_file, auth_file)
        user_ids = get_user_ids(infile)
        for user_id in user_ids:
            print('Retrieving {} for user {}.'.format(entity_type, user_id))
            outfile = '{}-{}.csv'.format(user_id, entity_type)
            outfile = os.path.join(outdir, outfile)
            if 'ids' in entity_type:
                users.retrieve_ids(user_id, entity_type, outfile)
            else:
                users.retrieve(user_id, entity_type, outfile)
    else:
        raise RuntimeError(
            'Either --user or --infile must be provided.')


def retrieve_friends(key_file, auth_file, user, outfile, infile, outdir):
    retrieve('friends', key_file, auth_file, user, outfile, infile, outdir)


def retrieve_followers(key_file, auth_file, user, outfile, infile, outdir):
    retrieve('followers', key_file, auth_file, user, outfile, infile, outdir)


def retrieve_friends_ids(key_file, auth_file, user, outfile, infile, outdir):
    retrieve('friends_ids', key_file, auth_file, user, outfile, infile, outdir)


def retrieve_followers_ids(key_file, auth_file, user, outfile, infile, outdir):
    retrieve(
        'followers_ids', key_file, auth_file, user, outfile, infile, outdir)
