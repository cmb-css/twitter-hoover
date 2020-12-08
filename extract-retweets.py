# coding=utf-8

import os
import glob
import gzip
import json
from collections import defaultdict
from hoover.users import get_user_ids


hashtags1 = {'#maréeverte', '#Verts', '#ZemmourFacts', '#Zemmour',
             '#municipales2020', '#bobos', '#Lyon', '#France'}

hashtags2 = {'#Aliot', '#RN', '#Perpignan', '#Municipales2020'}

hashtags3 = {'#remaniement', '#Remaniement', '#RemaniementMinisteriel',
             '#Justice', '#AFP', '#Castex', '#RemaniementDeLaHonte',
             '#Remaniement', '#remaniement', '#Macron', '#macron',
             '#14juillet', '#DupontMoretti', '#MeToo', '#balancetonporc',
             '#DeHaas', '#CultureDuViol', '#Darmanin', '#EricDupontMoretti',
             '#dupontmoretti', '#RemaniementMinisteriel', '#mondedapres'
             '#sijetaisprésident', '#Gdarmanin', '#RoselyneBachelot',
             '#NoHonorAmongThieves', '#Macronie', '#NoHonorAmongThieves',
             '#EmmanuelMacron', '#écolo', '#sociale', '#solidaire',
             '#EdouardPhilippe', '#gouvernement', '#démocratie', '#écologie',
             '#solidarité', '#Frexit', '#DeHaas', '#DupondMoretti',
             '#RemaniementMinistériel', '#mdp', '#mondedaprès', '#CETA',
             '#Mercosur', '#JETA', '#TAFTA', '#OTAN', '#Euroregion', '#GOPe',
             '#Art106destructionservicespublics', '#Frexit', '#Art50',
             '#Europe', '#France', '#Independance', '#Liberte', '#Democratie',
             '#censuredesmedias', '#bfmpolitique', '#TF1', '#franceinfo',
             '#franceinter', '#LCI', '#France24', '#France2', '#UPR',
             '#Asselineau', '#Coronavirus', '#Italexit', '#reveillezvous'}


hashtags = hashtags1 | hashtags2 | hashtags3


def filter_by_hashtags(json_str):
    for hashtag in hashtags:
        if hashtag in json_str:
            return True
    return False


def simple(tweet):
    return {
        'id': tweet['id_str'],
        'text': tweet['full_text'],
        'created_at': tweet['created_at'],
        'user': tweet['user']['screen_name']}


class ExtractRetweets(object):
    def __init__(self, infile, indir, outfile):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

        self.n_tweets = 0
        self.n_retweets = 0
        self.n_inretweets = 0

        self.tweets = {}
        self.retweets = defaultdict(list)
        self.parents = {}

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # TEMPORARY HACK!!! (DATE)
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '2020-07.json.gz'))
        return file_names

    def _reassign_parents(self):
        for root in self.retweets:
            parent = root
            while parent in self.parents:
                parent = self.parents[parent]
            if parent != root:
                self.retweets[parent] += self.retweets[root]
                self.retweets[root] = []

    def run(self):
        for i, user_id in enumerate(self.user_ids):
            print('processing user {} #{}/{}...'.format(
                user_id, i, len(self.user_ids)))
            for infile in self._user_files(user_id):
                print('infile: {}'.format(infile))
                with gzip.open(infile, 'rt') as f:
                    for line in f:
                        if filter_by_hashtags(line):
                            tweet = json.loads(line)
                            self.n_tweets += 1
                            if 'retweeted_status' in tweet:
                                self.n_retweets += 1
                                ruid = tweet['retweeted_status']['user']['id']
                                if ruid in self.user_ids:
                                    self.n_inretweets += 1

                                parent = tweet['retweeted_status']
                                parent_id = parent['id_str']
                                self.retweets[parent_id].append(simple(tweet))
                                self.tweets[parent_id] = simple(parent)
                                self.parents[tweet['id_str']] = parent_id
                                print()
                                print()
                                print('PARENT')
                                print(parent['full_text'])
                                print('RETWEET')
                                print(tweet['full_text'])
                                print()
                                print()

                print('# tweets: {}; # retweets: {}; # inretweets: {}'.format(
                    self.n_tweets, self.n_retweets, self.n_inretweets))
                print('inretweet ratio: {}'.format(
                    float(self.n_inretweets) / float(self.n_retweets)))

        self._reassign_parents()

        with open(self.outfile, 'wt') as f:
            for tid in self.tweets:
                if len(self.retweets[tid]) > 0:
                    tweet = self.tweets[tid]
                    tweet['retweets'] = self.retweets[tid]
                    f.write('{}\n'.format(json.dumps(tweet)))


if __name__ == '__main__':
    tr = ExtractRetweets(
        'retweet-userids.csv', 'timelines', 'retweets.json')
    tr.run()
