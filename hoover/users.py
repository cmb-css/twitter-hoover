from hoover.auth import twython_from_key_and_auth
from hoover.rate_control import RateControl


class Users(RateControl):
    def __init__(self, key_file, auth_file):
        super().__init__()
        self.twitter = twython_from_key_and_auth(key_file, auth_file)

    def screen_name2id(self, screen_name):
        self.pre_request(verbose=True)
        response = self.twitter.lookup_user(screen_name=screen_name)
        return response[0]['id']

    def retrieve(self, screen_name, request_type, outfile):
        user_id = self.screen_name2id(screen_name)
        ids = []
        cursor = -1
        while cursor != 0:
            self.pre_request(verbose=True)
            if request_type == 'friends':
                response = self.twitter.get_friends_ids(user_id=user_id,
                                                        cursor=cursor)
            elif request_type == 'followers':
                response = self.twitter.get_followers_ids(user_id=user_id,
                                                          cursor=cursor)
            else:
                raise RuntimeError(
                    'unknown request type "{}"'.format(request_type))
            cursor = response['next_cursor']
            ids += response['ids']

        with open(outfile, 'w') as f:
            f.write('\n'.join([str(x) for x in ids]))
            f.write('\n')
        print('{} found.'.format(len(ids)))


def retrieve_friends(key_file, auth_file, screen_name, outfile):
    Users(key_file, auth_file).retrieve(screen_name, 'friends', outfile)


def retrieve_followers(key_file, auth_file, screen_name, outfile):
    Users(key_file, auth_file).retrieve(screen_name, 'followers', outfile)
