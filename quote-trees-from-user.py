import argparse
import glob
import gzip
import json
import os
from hoover.users import get_user_ids


def _simple(tweet):
    text = tweet['full_text'] if 'full_text' in tweet else tweet['text']
    data = {
        'id': tweet['id_str'],
        'root': True,
        'text': text,
        'created_at': tweet['created_at'],
        'user': tweet['user']['screen_name'],
        'user_id': tweet['user']['id'],
        'followers_count': tweet['user']['followers_count'],
        'friends_count': tweet['user']['friends_count'],
        'is_quote': tweet['is_quote_status'],
        'urls': [],
        'hashtags': [],
        'in_reply_to_user': tweet['in_reply_to_screen_name'],
        'in_reply_to_user_id': tweet['in_reply_to_user_id_str'],
        'in_reply_to_status_id': tweet['in_reply_to_status_id_str'],
        'quotes': [],
        'quote_ids': []}
    if 'entities' in tweet:
        if 'urls' in tweet['entities']:
            for url_entity in tweet['entities']['urls']:
                data['urls'].append(url_entity['expanded_url'])
        if 'hashtags' in tweet['entities']:
            for hashtag_entity in tweet['entities']['hashtags']:
                data['hashtags'].append(hashtag_entity['text'])
    return data


class QuoteTreesFromUser:
    def __init__(self, indir, outfile, userid):
        # self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile
        self.userid = userid

        self.n_trees = 0
        self.n_quotes = 0

        self.tweets = {}
        self.root_tweets = {}

    def _filter(self, json_str):
        return 'quoted_status' in json_str

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        return glob.glob(os.path.join(self._user_path(user_id), '*.json.gz'))

    def _process_file2(self, infile):
        with gzip.open(infile, 'rt') as f:
            for line in f:
                if self._filter(line):
                    try:
                        tweet = json.loads(line)
                        if 'quoted_status' in tweet:
                            tweet_id = tweet['id_str']
                            qs = tweet['quoted_status']
                            quid = qs['user']['id']
                            if quid in self.user_ids:
                                self.n_quotes += 1
                                parent = qs
                                parent_id = parent['id_str']

                                # add parent if it does not exist yet
                                if parent_id not in self.tweets:
                                    sparent = _simple(parent)
                                    self.tweets[parent_id] = sparent
                                    if not sparent['is_quote']:
                                        self.n_trees += 1

                                if tweet_id not in self.tweets:
                                    self.tweets[tweet_id] = _simple(tweet)
                                self.tweets[tweet_id]['root'] = False
                                ptweet = self.tweets[parent_id]
                                if tweet_id not in ptweet['quote_ids']:
                                    ptweet['quotes'].append(
                                        self.tweets[tweet_id])
                                    ptweet['quote_ids'].append(tweet_id)
                    except json.decoder.JSONDecodeError:
                        pass

    def _process_file(self, infile):
        print('processing file: {}'.format(infile))
        with gzip.open(infile, 'rt') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                    tweet_id = tweet['id_str']
                    # if 'quoted_status' in tweet:
                    #     qs = tweet['quoted_status']
                    #     quid = qs['user']['id']

                    self.tweets[tweet_id] = tweet
                    if 'quoted_status' not in tweet:
                        self.root_tweets[tweet_id] = tweet
                except json.decoder.JSONDecodeError:
                    pass

    def run(self):
        for infile in self._user_files(self.userid):
            self._process_file(infile)

        print('#tweets: {} | #root_tweets: {}'.format(len(self.tweets), len(self.root_tweets)))

        # for i, user_id in enumerate(self.user_ids):
        #     print('processing user {} #{}/{}...'.format(
        #         user_id, i, len(self.user_ids)))
        #     for infile in self._user_files(user_id):
        #         self._process_file(infile)
        #     print('trees: {}; quotes: {}'.format(self.n_trees, self.n_quotes))

        # write trees
        # with open(self.outfile, 'wt', encoding='utf-8') as f:
        #     for tid, tweet in self.tweets.items():
        #         if tweet['root']:
        #             f.write('{}\n'.format(
        #                 json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--indir', type=str, help='directory with timelines', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument('--userid', type=str, help='user id', default=None)
    args = parser.parse_args()

    indir = args.indir
    outfile = args.outfile
    userid = args.userid

    print('indir: {}'.format(indir))
    print('outfile: {}'.format(outfile))
    print('user id: {}'.format(userid))

    qtfu = QuoteTreesFromUser(indir, outfile, userid)
    qtfu.run()
