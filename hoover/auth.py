from twython import Twython


def read_strings_from_file(file_path, how_many):
    with open(file_path, 'r') as file:
        data = file.read()
    return data.split()[:how_many]


def read_key_and_secret(file_path):
    return read_strings_from_file(file_path, 2)


def read_token_secret_pin(file_path):
    return read_strings_from_file(file_path, 2)


def write_token_secret(file_path, token, secret):
    with open(file_path, 'w') as file:
        file.write("{}\n{}".format(token, secret))


def auth_app(key_file, auth_file):
    app_key, app_secret = read_key_and_secret(key_file)

    # obtaining URL for authentication
    twitter = Twython(app_key, app_secret)
    auth = twitter.get_authentication_tokens()
    oauth_token = auth['oauth_token']
    oauth_token_secret = auth['oauth_token_secret']

    # request pin
    print('Go here: {}'.format(auth['auth_url']))
    pin = input('PIN? ')

    # complete authorization with PIN
    twitter = Twython(app_key, app_secret, oauth_token, oauth_token_secret)
    auth = twitter.get_authorized_tokens(pin)
    oauth_token = auth['oauth_token']
    oauth_token_secret = auth['oauth_token_secret']

    # write token and secret to file
    write_token_secret(auth_file, oauth_token, oauth_token_secret)
    print('auth credentials written to: {}'.format(auth_file))
