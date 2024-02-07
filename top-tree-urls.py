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
    parser.add_argument('--infile', type=str, help='input file', default=None)
    parser.add_argument('--outfile', type=str, help='output file', default=None)
    parser.add_argument('--minsize', type=int, help='minimum number of tree nodes', default=5)
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    minsize = args.minsize
    
    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('minsize: {}'.format(minsize))
    
    with open(infile, 'rt') as f:
        trees = [json.loads(line) for line in f]
    
    n = 0
    _n = 0
    with open(infile, 'rt') as in_f, open(outfile, 'wt') as out_f:
        for line in in_f:
            n += 1
            tree = json.loads(line)
            size, _ = tree_metrics(tree)
            if size >= minsize:
                _n += 1
                url = 'https://twitter.com/i/web/status/{}'.format(str(tree['id']))
                out_f.write(line)

    print(f'{_n} out of {n} trees preserved.')
