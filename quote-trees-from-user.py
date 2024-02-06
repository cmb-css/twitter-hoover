import argparse
import gzip
import json


class QuoteTreesFromUser:
    def __init__(self, infile, outfile, user_id):
        self.infile = infile
        self.outfile = outfile
        self.user_id = user_id

        self.trees = []

    def run(self):
        with gzip.open(self.infile, 'rt') as f:
            for line in f:
                try:
                    if self.user_id in line:
                        tweet = json.loads(line)
                        if tweet['user_id'] == user_id:
                            self.trees.append(tweet)
                            print(len(self.trees))
                except json.decoder.JSONDecodeError:
                    pass

        # write trees
        # with open(self.outfile, 'wt', encoding='utf-8') as f:
        #     for tid, tweet in self.tweets.items():
        #         if tweet['root']:
        #             f.write('{}\n'.format(
        #                 json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument('--userid', type=str, help='user id', default=None)
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    user_id = args.userid

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('user id: {}'.format(user_id))

    qtfu = QuoteTreesFromUser(infile, outfile, user_id)
    qtfu.run()
