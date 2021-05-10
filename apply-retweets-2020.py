import json


def get_retweets(retweets_file):
    with open(retweets_file, 'rt') as f:
        json_str = f.read()
    return json.loads(json_str)


class ApplyRetweets:
    def __init__(self, infile, outfile, retweets_file):
        self.infile = infile
        self.outfile = outfile
        self.retweets = get_retweets(retweets_file)

    def apply_retweets(self, tree):
        if 'retweeters' not in tree:
            tree['retweeters'] = []
        if tree['id'] in self.retweets:
            tree['retweeters'] += self.retweets[tree['id']]
        for quote in tree['quotes']:
            self.apply_retweets(quote)

    def run(self):
        with open(self.infile, 'rt', encoding='utf-8') as in_f,\
             open(self.outfile, 'wt', encoding='utf-8') as out_f:
            for line in in_f:
                tree = json.loads(line)
                self.apply_retweets(tree)
                out_f.write(
                    '{}\n'.format(json.dumps(tree, ensure_ascii=False)))


if __name__ == '__main__':
    ar = ApplyRetweets(
        'quotes-2020.json', 'quotes-2020+retweets.json', 'retweets-2020.json')
    ar.run()
