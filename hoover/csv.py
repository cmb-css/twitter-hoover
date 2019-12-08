import csv
from os import listdir
from os.path import isfile, join
from hoover.simple import read_simple


FIELDS_TWEET = ('created_at',
                'timestamp',
                'id',
                'text',
                'retweet_count',
                'favorite_count',
                'lang')


FIELDS_USER = ('user_id', 'user_screen_name')


FIELDS_ALL = ('reply', 'retweet', 'quote')


FIELDS_REPLY = ('in_reply_to_status_id',
                'in_reply_to_user_id',
                'in_reply_to_screen_name')


FIELDS_PARENT_TWEET = ('quoted_text',)


FIELDS_RETWEET = ('retweeted_id',
                  'retweeted_user_id',
                  'retweeted_user_screen_name')


FIELDS_QUOTE = ('quoted_id',
                'quoted_user_id',
                'quoted_user_screen_name')


def _matches_filter(csv_type, tweet):
    if csv_type in {'all', 'hashtags', 'mentions'}:
        return True
    elif csv_type == 'tweets':
        return ((not tweet['reply']) and
                (not tweet['retweet']) and
                (not tweet['quote']))
    elif csv_type == 'replies':
        return tweet['reply']
    elif csv_type == 'retweets':
        return tweet['retweet']
    elif csv_type == 'quotes':
        return tweet['quote']
    raise RuntimeError('Unknown csv type: {}.'.format(csv_type))


def tweets_to_csv(tweets, outfile, csv_type='all', user_data=True):
    base_fields = FIELDS_TWEET
    if user_data:
        base_fields += FIELDS_USER

    if csv_type == 'all':
        fields = (base_fields + FIELDS_PARENT_TWEET + FIELDS_ALL +
                  FIELDS_REPLY + FIELDS_RETWEET + FIELDS_QUOTE)
    elif csv_type == 'tweets':
        fields = base_fields
    elif csv_type == 'replies':
        fields = base_fields + FIELDS_REPLY
    elif csv_type == 'retweets':
        fields = base_fields + FIELDS_PARENT_TWEET + FIELDS_RETWEET
    elif csv_type == 'quotes':
        fields = base_fields + FIELDS_PARENT_TWEET + FIELDS_QUOTE
    else:
        raise RuntimeError('Unknown csv type: {}'.format(csv_type))

    if user_data:
        fields += FIELDS_USER

    with open(outfile, 'w') as outfile:
        csvwriter = csv.writer(outfile)
        csvwriter.writerow(fields)
        for tweet in tweets:
            csvwriter.writerow([tweet[field] for field in fields])

    return 1


def hashtags(tweets, outfile, user_data):
    counts = {}
    for tweet in tweets:
        user = tweet['user_screen_name']
        if user not in counts:
            counts[user] = {}
        for occurrence in tweet['hashtags']:
            if occurrence not in counts[user]:
                counts[user][occurrence] = 0
            counts[user][occurrence] += 1

    if len(counts) == 0:
        return 0

    fields = ('hashtag', 'occurrences')
    if user_data:
        fields = ('user',) + fields

    with open(outfile, 'w') as outfile:
        csvwriter = csv.writer(outfile)
        csvwriter.writerow(fields)

        for user in counts:
            for occurrence in counts[user]:
                row = {'user': user,
                       'hashtag': occurrence,
                       'occurrences': counts[user][occurrence]}
                csvwriter.writerow([row[field] for field in fields])

    return 1


def mentions(tweets, outfile, user_data):
    counts = {}
    for tweet in tweets:
        user = tweet['user_screen_name']
        if user not in counts:
            counts[user] = {}
        for occurrence in tweet['mentions']:
            if occurrence not in counts[user]:
                counts[user][occurrence] = 0
            counts[user][occurrence] += 1

    if len(counts) == 0:
        return 0

    fields = ('mentioned_id', 'mentioned_screen_name', 'occurrences')
    if user_data:
        fields = ('user',) + fields

    with open(outfile, 'w') as outfile:
        csvwriter = csv.writer(outfile)
        csvwriter.writerow(fields)

        for user in counts:
            for occurrence in counts[user]:
                row = {'user': user,
                       'mentioned_id': occurrence[0],
                       'mentioned_screen_name': occurrence[1],
                       'occurrences': counts[user][occurrence]}
                csvwriter.writerow([row[field] for field in fields])

    return 1


def json_file_to_csv(infile, outfile, csv_type='all', user_data=True):
    tweets = tuple(tweet for tweet in read_simple(infile)
                   if _matches_filter(csv_type, tweet))
    if len(tweets) == 0:
        return 0

    if csv_type == 'hashtags':
        return hashtags(tweets, outfile, user_data)
    elif csv_type == 'mentions':
        return mentions(tweets, outfile, user_data)
    else:
        return tweets_to_csv(tweets, outfile, csv_type, user_data)


def dir_to_csvs(indir, outdir, csv_type='all'):
    files = [f for f in listdir(indir) if isfile(join(indir, f))]
    n = 0
    for file in files:
        if file[-5:] == '.json':
            infile = join(indir, file)
            outfile = '{}-{}.csv'.format(file[:-5], csv_type)
            outfile = join(outdir, outfile)
            n += json_file_to_csv(
                infile, outfile, csv_type, user_data=False)
    return n


def to_csv(infile, outfile, indir, outdir, csv_type):
    if csv_type:
        filters = {csv_type}
    else:
        filters = ('all', 'tweets',
                   'replies', 'retweets', 'quotes',
                   'hashtags', 'mentions')

    print('Using filters: {}.'.format(', '.join(filters)))

    n = 0
    if indir:
        if infile:
            raise RuntimeError(
                'Only one of --infile or --indir should be provided.')
        if outfile:
            raise RuntimeError(
                'Only one of --outfile or --indir should be provided.')
        if not outdir:
            raise RuntimeError('--outdir must be provided.')
        for filt in filters:
            print('Converting to csv type: {}'.format(filt))
            n += dir_to_csvs(indir, outdir, filt)
    elif infile:
        if indir:
            raise RuntimeError(
                'Only one of --infile or --indir should be provided.')
        if outdir:
            raise RuntimeError(
                'Only one of --infile or --outdir should be provided.')
        if not outfile:
            raise RuntimeError('--outfile must be provided.')
        for filt in filters:
            print('Converting to csv type: {}'.format(filt))
            n += json_file_to_csv(infile, outfile, filt)
    else:
        raise RuntimeError('Either --infile or --indir must be provided.')

    print('{} csv files created.'.format(str(n)))
