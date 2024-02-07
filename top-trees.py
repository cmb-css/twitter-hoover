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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='file with all userids', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    
    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    
    with open(infile, 'rt') as f:
        trees = [json.loads(line) for line in f]
    
    metrics = [tree_metrics(tree) for tree in trees]

    print('By size:')
    print(sorted(metrics, key=lambda x: x[0], reverse=True))

    print('By depth:')
    print(sorted(metrics, key=lambda x: x[1], reverse=True))
