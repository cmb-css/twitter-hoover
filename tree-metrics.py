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


def extract_users(tree):
    users = {tree['user']}
    for quote in tree['quotes']:
        users |= extract_users(quote)
    return users


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    args = parser.parse_args()

    infile = args.infile
    
    print('infile: {}'.format(infile))
    
    with open(infile, 'rt') as f:
        trees = [json.loads(line) for line in f]
    
    users = set()
    metrics = []
    for tree in trees:
        size, depth = tree_metrics(tree)
        if size >= 4:
            users |= extract_users(tree)
            metrics.append((size, depth))

    metrics = sorted(metrics, key=lambda x: x[0], reverse=True)

    print('#distinct users: {}'.format(len(users)))
    print('sizes:')
    print(', '.join([str(metric[0]) for metric in metrics]))
