# twitter-hoover
Retrieve data from Twitter.

This is a general utility to retrieve data from Twiter, meant to be used from the command line. Below are some instructions on how to install it, and how to perform the most common tasks.

## How to install

Clone this repository into your local machine:

`git clone https://github.com/cmb-css/twitter-hoover.git`

You can then `cd` into the created directory and use `pip` to install locally:

`pip install .`

## How to use

### Authorize the app (auth)

To authorize the app, first you need to create the local file `key-and-secret.txt`. This file should contain your private Twitter APP_KEY and APP_SECRET (one per line, nothing else). You can obrain these at https://developer.twitter.com. Then simply execute the following command:

`hoover auth`

You will be give a URL to perform authorization. After you authorize the app on this page, you will be given a PIN code, that you should then insert in the command line (there will be a prompt asking you for this). Both OAUTH_TOKEN and OAUTH_TOKEN_SECRET will be written to the local file `auth.txt`. This file will then be available for other commands to transparently perform authentication.

### Read a filtered stream into a local file (stream)

`hoover --infile <keywords_file> --outfile <outfile> stream`

JSON data will be written to `<outfile>`. The stream will be filtered by the keywords or hashtags found in `<keywords_file>` (one keyword/hashtag per line).

Tweets are saved one per line, in the form of the full JSON object reveived from the Twitter API.

### Retrieve tweets from user timelines (timelines)

This allows for the retrieval of tweets from the timelines of the specified users. The simplest way to use it is to specify either a screen name or user id:

`hoover --user <screen_name or user_id> --outfile <filename> timelines`

You can also provide a list of users as an input file (one user_id per line):

`hoover --infile <users> --outdir <outdir> timelines`

In this case, the input file is interpreted as a .csv file. It can have a header or not (this is automatically detected), and the first row is assumed to contain the user ids for which data should be collected. There can be an arbitrary number of other rows, they are ignored here. The outputs of the `folowers` and `friends` commands, for example, are valid input files for this commmand. One file will be created for each user, inside `outdir`, and its name will be the respective user id with the extension `.json`.


Tweets are saved one per line, in the form of the full JSON object reveived from the Twitter API.

### Retrieve followers (followers)

This allows for the retrieval of information on the followers of the specified users. The simplest way to use it is to specify either a screen name or user id:

`hoover --user <screen_name or user_id> --outfile <filename> followers`

You can also provide a list of users as an input file (one user_id per line):

`hoover --infile <users> --outdir <outdir> followers`

In this case, the input file is interpreted as a .csv file. It can have a header or not (this is automatically detected), and the first row is assumed to contain the user ids for which data should be collected. There can be an arbitrary number of other rows, they are ignored here. The outputs of the `folowers` and `friends` commands, for example, are valid input files for this commmand. One file will be created for each user, inside `outdir`, and its name will follow the template: `<user_id>-followers.csv`.


The output file(s) are .csv files, with each line containing information about one user. Its columns are: id, screen_name, name, location, protected, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, created_ts.


### Retrieve friends (friends)

This allows for the retrieval of information on the friends of the specified users. The simplest way to use it is to specify either a screen name or user id:

`hoover --user <screen_name or user_id> --outfile <filename> friends`

You can also provide a list of users as an input file (one user_id per line):

`hoover --infile <users> --outdir <outdir> friends`

In this case, the input file is interpreted as a .csv file. It can have a header or not (this is automatically detected), and the first row is assumed to contain the user ids for which data should be collected. There can be an arbitrary number of other rows, they are ignored here. The outputs of the `folowers` and `friends` commands, for example, are valid input files for this commmand. One file will be created for each user, inside `outdir`, and its name will follow the template: `<user_id>-friends.csv`.


The output file(s) are .csv files, with each line containing information about one user. Its columns are: id, screen_name, name, location, protected, verified, followers_count, friends_count, listed_count, favourites_count, statuses_count, created_at, created_ts.

### Convert to .csv (csv)

This command converts tweets encoded as raw JSON objets (as received from the Twitter API) to .csv files. The simplest way to use it is to specify an input and an output file:

`hoover --infile <input file with JSON objects> --outfile <output .csv file> [--type <csv_type>] csv`

You can also provide input and output directories:

`hoover --indir <directory with input .json files> --outdir <directory with output .csv files> [--type <csv_type>] csv`

In this case, .json file names are assumed to be the user ids of the author of the tweets they contain. These user ids will be used to automatically generate the names of the output .csv files.

There are several types of .csv files that can be generated. We list them here, by the name that can be specified with the optioanl `--type` parameter:

* *all*: all the tweets (including replies, retweets and quotes)
* *tweets*: only simple tweets (no replies, retweets or quotes)
* *replies*: only replies
* *retweets*: only retweets
* *quotes*: only quotes
* *hashtags*:  all the hastags contained in the tweets, including number of occurrences
* *mentions*: all the mentions contained in the tweets, including number of occurrences

If `--type` is not specified, all of the above outputs are generated. Output files that have automatically generated names (using `--indir` and `--outdir`), will be identified with one of the above csv types as a suffix. To illustrate, if there is a file called `4135510295844.json` on the input directory, then  the following files will be generated on the output directory:

* 4135510295844-all.csv
* 4135510295844-tweets.csv
* 4135510295844-replies.csv
* 4135510295844-retweets.csv
* 4135510295844-quotes.csv
* 4135510295844-hashtags.csv
* 4135510295844-mentions.csv

Or only one of them, if `--type` is specified.

All the .csv files that directly list tweets (the first 5 types above) have the following columns: 'created_at', 'timestamp', 'id', 'text', 'retweet_count', 'favorite_count' and 'lang'. If the user who created the tweet is not speficied in the file name (this is the case when one input and one output file are directly specified, as in the first example of invocation of the command above), then two extra columns are present: 'user_id' and 'user_screen_name'. Files that contain replies include the columns: 'in_reply_to_status_id', 'in_reply_to_user_id' and 'in_reply_to_screen_name'. Files that contin tweets quoting parent tweets (retweets and quotes) include the column 'quoted_text'. Files  that contain retweets include the fileds: 'retweeted_id', 'retweeted_user_id' and 'retweeted_user_screen_name'. Files that contain quotes include the fileds: 'quoted_id', 'quoted_user_id' and 'quoted_user_screen_name'.

### Simplify JSON (simplify)

JSON objects produced by the JSON API to represent Tweets can be a bit complex. This converts them to a flatter, simpler JSON schema that might be simpler to user from many purposes:

`hoover --infile <input file with Tweet JSON objects> --outfile <output .json file> simplify`

### Extract YouTube videos (youtube)

This command extracts all the URLs corresponding to YouTube videos from a list of Tweets in API JSON format (e.g. the ones outputted by the `stream` or `timelines` commands).

`hoover --infile <input file with Tweets JSON objects> --outfile <output .csv file> youtube`


The output files is a .csv file with two columns: video URL and number of occurrences.
