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
    def __init__(self, infile, outfile, user):
        self.infile = infile
        self.outfile = outfile
        self.user = user
        self.max_size = -1
        self.max_depth = -1

        self.trees = []

    def _filter(self, tree):
        text = tree['text'].lower()
        if 'covid' in text:
            return True
        if 'corona' in text:
            return True
        if 'mask' in text:
            return True
        if 'impf' in text:
            return True
        if 'vaccine' in text:
            return True
        return False

    def run(self):
        with open(self.infile, 'rt') as f:
            for line in f:
                try:
                    if self.user in line:
                        tree = json.loads(line)
                        if str(tree['user']) == self.user and self._filter(tree):
                            size, depth = tree_metrics(tree)
                            if size > self.max_size:
                                self.max_size = size
                            if depth > self.max_depth:
                                self.max_depth = depth
                            self.trees.append(tree)
                except json.decoder.JSONDecodeError:
                    pass

        print('#trees: {}'.format(len(self.trees)))
        print('max size: {} | max depth: {}'.format(self.max_size, self.max_depth))

        # write trees
        with open(self.outfile, 'wt', encoding='utf-8') as f:
            for tree in self.trees:
                f.write('{}\n'.format(json.dumps(tree, ensure_ascii=False)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument('--user', type=str, help='user name', default=None)
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    user = args.user

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('user: {}'.format(user))

    qtfu = QuoteTreesFromUser(infile, outfile, user)
    qtfu.run()
