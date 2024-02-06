import argparse
import json


def tree_metrics(tweet):
    size = 1
    max_depth = 0
    for quote in tweet['quotes']:
        _size, _depth = tree_metrics(quote)
        size += _size
        if _depth > max_depth:
                max_depth = _depth
    return size, max_depth + 1


class QuoteTreesFromUser:
    def __init__(self, infile, outfile, user_id):
        self.infile = infile
        self.outfile = outfile
        self.user_id = user_id
        self.max_size = -1
        self.max_depth = -1

        self.trees = []

    def _filter(self, line):
        lline = line.lower()
        if 'covid' in lline:
            return True
        if 'corona' in lline:
            return True
        if 'mask' in lline:
            return True
        if 'impf' in lline:
            return True
        if 'vaccine' in lline:
            return True
        return False

    def run(self):
        with open(self.infile, 'rt') as f:
            for line in f:
                if self._filter(line):
                    try:
                        if self.user_id in line:
                            tweet = json.loads(line)
                            if str(tweet['user_id']) == self.user_id:
                                size, depth = tree_metrics(tweet)
                                if size > self.max_size:
                                    self.max_size = size
                                if depth > self.max_depth:
                                    self.max_depth = depth
                                self.trees.append(tweet)
                    except json.decoder.JSONDecodeError:
                        pass

        print('#trees: {}'.format(len(self.trees)))
        print('max size: {} | max depth: {}'.format(self.max_size, self.max_depth))

        # write trees
        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for tweet in self.tweets:
                f.write('{}\n'.format(json.dumps(tweet, ensure_ascii=False)))


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
