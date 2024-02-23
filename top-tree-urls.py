import argparse
import json

from hoover.anon.decrypt_indiv import deanonymize


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
    parser.add_argument('--ntrees', type=int, help='number of trees', default=30)
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    args = parser.parse_args()

    infile = args.infile
    outfile = args.outfile
    ntrees = args.ntrees
    
    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))
    print('#trees: {}'.format(ntrees))
    
    
    trees = []
    with open(infile, 'rt') as in_f:
        for line in in_f:
            tree = json.loads(line)
            size, _ = tree_metrics(tree)
            trees.append((tree, size))
    trees = sorted(trees, key=lambda x: x[1], reverse=True)

    with open(outfile, 'wt') as out_f:
        for tree in trees[:ntrees]:
            tid = deanonymize(str(tree['id']), args.anon_db_folder_path)
            url = 'https://twitter.com/i/web/status/{}'.format(tid)
            out_f.write(f'{url}\n')
