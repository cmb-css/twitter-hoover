# twitter-hoover
Retrieve data from Twitter.

## How to install

Clone this repository into your local machine:

`git clone https://github.com/cmb-css/twitter-hoover.git`

You can then `cd` into the created directory and use `pip` to install locally:

`pip install .`

## How to use

### Authorize the app

To authorize the app, first you need to create the local file `key-and-secret.txt`. This file should contain your private Twitter APP_KEY and APP_SECRET (one per line, nothing else). You can obrain these at https://developer.twitter.com. Then simply execute the following command:

`python -m hoover auth`

You will be give a URL to perform authorization. After you authorize the app on this page, you will be given a PIN code, that you should then insert in the command line (there will be a prompt asking you for this). Both OAUTH_TOKEN and OAUTH_TOKEN_SECRET will be written to the local file `auth.txt`. This file will then be available for other commands to transparently perform authentication.

### Read a filtered stream into a local file

`python -m hoover --infile <keywords_file> --outfile <outfile> stream`

JSON data will be written to `<outfile>`. The stream will be filtered by the keywords or hashtags found in `<keywords_file>` (one keyword/hashtag per line).

### Retrieve tweets from user timelines

This allows for the retrieval of tweets from the timelines of users listed in an input file (one user_id per line):

`python -m hoover --infile <keywords_file> --outdir <outdir> timelines`