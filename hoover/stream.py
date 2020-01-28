import time
import json
from twython import TwythonStreamer
from hoover.auth import read_key_and_secret, read_token_secret_pin
from hoover.filter import create_filter


class HooverStreamer(TwythonStreamer):
    def __init__(self, outfile, errfile, app_key, app_secret, oauth_token,
                 oauth_token_secret):
        super().__init__(app_key, app_secret, oauth_token, oauth_token_secret)
        self.outfile = outfile
        self.errfile = errfile

    def on_success(self, data):
        with open(self.outfile, 'a') as file:
            file.write('{}\n'.format(json.dumps(data)))

    def on_error(self, status_code, data):
        print('ERROR {}: {}'.format(status_code, data))
        with open(self.errfile, 'a') as file:
            file.write('ERROR {}: {}\n'.format(status_code, data))


def _read_stream(key_file, auth_file, keywords_file, outfile, errfile):
    app_key, app_secret = read_key_and_secret(key_file)
    oauth_token, oauth_token_secret = read_token_secret_pin(auth_file)

    stream = HooverStreamer(outfile, errfile, app_key, app_secret,
                            oauth_token, oauth_token_secret)
    terms = create_filter(keywords_file)
    print(terms)
    stream.statuses.filter(track=terms)


def read_stream(key_file, auth_file, keywords_file, outfile, errfile):
    while True:
        try:
            _read_stream(key_file, auth_file, keywords_file, outfile, errfile)
        except Exception as err:
            with open(errfile, 'a') as file:
                file.write('EXCEPTION {}\n'.format(err))
            # sleep for one minute and try again
            time.sleep(60)
