import json


class ExtractQuotesIds:
    def __init__(self, infile, outfile):
        self.infile = infile
        self.outfile = outfile
        self.tweet_ids = set()

    def process_tweet(self, tree):
        self.tweet_ids.add(tree['id'])
        for quote in tree['quotes']:
            self.process_tweet(quote)

    def run(self):
        with open(self.infile, 'rt', encoding='utf-8') as f:
            for line in f:
                tree = json.loads(line)
                self.process_tweet(tree)

        with open(self.outfile, 'wt') as f:
            for tid in self.tweet_ids:
                f.write('{}\n'.format(tid))


if __name__ == '__main__':
    eqids = ExtractQuotesIds('quotes-2020.json', 'quotes-2020-tweet-ids.csv')
    eqids.run()
