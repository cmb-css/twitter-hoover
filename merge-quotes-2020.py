import json


def month2file(month):
    return 'quotes-2020-{:02}.json'.format(month)


class MergeQuotes:
    def __init__(self, outfile):
        self.outfile = outfile
        self.root_ids = set()
        self.cur_tweets = {}

    def _add_quotes(self, tweet):
        for quote in tweet['quotes']:
            tid = quote['id']
            if tid not in self.root_ids:
                self.cur_tweets[tid] = quote
                self.root_ids.add(tid)
            self._add_quotes(quote)

    def _month_tweets(self, month):
        self.cur_tweets = {}
        with open(month2file(month), 'rt') as f:
            for line in f:
                tweet = json.loads(line)
                tid = tweet['id']
                if not tweet['is_quote'] and tid not in self.root_ids:
                    self.cur_tweets[tid] = tweet
                    self.root_ids.add(tid)
                    self._add_quotes(tweet)

    def _merge_trees(self, month):
        for i in range(month + 1, 13):
            with open(month2file(i), 'rt') as f:
                for line in f:
                    tweet = json.loads(line)
                    tid = tweet['id']
                    if tid in self.cur_tweets:
                        main_tweet = self.cur_tweets[tid]
                        for quote in tweet['quotes']:
                            qid = quote['id']
                            if qid not in main_tweet['quote_ids']:
                                main_tweet['quotes'].append(quote)
                                main_tweet['quote_ids'].append(qid)

    def run(self):
        for i in range(1, 13):
            print('processing month: {}'.format(i))
            self._month_tweets(i)
            self._merge_trees(i)
            with open(self.outfile, 'at', encoding='utf-8') as f:
                for tid, tweet in self.cur_tweets.items():
                    if not tweet['is_quote']:
                        f.write('{}\n'.format(
                            json.dumps(tweet, ensure_ascii=False)))


if __name__ == '__main__':
    mq = MergeQuotes('quotes-2020.json')
    mq.run()
