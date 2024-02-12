import argparse
import json
from collections import defaultdict


def tree_metrics(tweet, users=None):
    if users is None:
        users = set()
    users.add(tweet['user_id'])
    size = 1
    max_depth = 0
    for quote in tweet['quotes']:
        _size, _depth = tree_metrics(quote, users)
        size += _size
        if _depth > max_depth:
                max_depth = _depth
    return size, max_depth + 1, users


class QuoteTreesByUser:
    def __init__(self, infile, outfile, perimeter):
        self.infile = infile
        self.outfile = outfile

        with open(perimeter, 'rt') as f:
            self.perimeter = [line.strip() for line in perimeter]
        print(self.perimeter)
        print(f'Perimeter of {len(self.perimeter)} user loaded.')

        self.max_size = -1
        self.max_depth = -1
        self.sizes = {}

        self.tree_metrics = defaultdict(list)
        self.users = defaultdict(set)

    def _filter(self, text):
        text = text.lower()
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
                    if self._filter(line):
                        tree = json.loads(line)
                        if self._filter(tree['text']) and tree['user_id'] in self.perimeter:
                            size, depth, users = tree_metrics(tree)
                            if size > self.max_size:
                                self.max_size = size
                                print(f'new max size found: {self.max_size}')
                            if depth > self.max_depth:
                                self.max_depth = depth
                                print(f'new max depth found: {self.max_depth}')
                            self.tree_metrics[str(tree['user'])].append((size, depth, users))
                except json.decoder.JSONDecodeError:
                    pass

        print('max size: {} | max depth: {}'.format(self.max_size, self.max_depth))

        # compute top trees metrics
        for user in self.tree_metrics:
            self.tree_metrics[user] = sorted(self.tree_metrics[user], key=lambda x: x[0], reverse=True)[:15]

        # user list by top trees size
        users = sorted(self.tree_metrics.keys(),
                       key=lambda x: sum([metrics[0] for metrics in self.tree_metrics[x]]),
                       reverse=True)

        # write csv
        with open(self.outfile, 'wt', encoding='utf-8') as f:
            f.write('user,{},unique_users,max_depth\n'.format(','.join([f'tree{i}' for i in range(1, 16)])))
            for user in users:
                sizes = []
                unique_users = set()
                max_depth = -1
                for size, depth, tree_users in self.tree_metrics[user]:
                    sizes.append(str(size))
                    unique_users |= set(tree_users)
                    if depth > max_depth:
                        max_depth = depth
                sizes += ['0'] * (15 - len(sizes))
                f.write('{},{},{},{}\n'.format(user, ','.join(sizes), unique_users, max_depth))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument('--perimeter', type=str, help='list of accepted users', default=None)
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    perimeter = args.perimeter

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('perimeter: {}'.format(perimeter))

    qtbu = QuoteTreesByUser(infile, outfile, perimeter)
    qtbu.run()
