from twython import Twython


def read_key_and_secret(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    return data.split()[:2]


def write_token_secret_pin(file_path, token, secret, pin):
    with open(file_path, 'w') as file:
        file.write("{}\n{}\n{}".format(token, secret, pin))


def auth_app(key_file, auth_file):
    APP_KEY, APP_SECRET = read_key_and_secret(key_file)
    twitter = Twython(APP_KEY, APP_SECRET)
    auth = twitter.get_authentication_tokens()
    OAUTH_TOKEN = auth['oauth_token']
    OAUTH_TOKEN_SECRET = auth['oauth_token_secret']
    print('Go here: {}'.format(auth['auth_url']))
    PIN = input('PIN? ')
    write_token_secret_pin(auth_file, OAUTH_TOKEN, OAUTH_TOKEN_SECRET, PIN)
    print('auth credentials written to: {}'.format(auth_file))
