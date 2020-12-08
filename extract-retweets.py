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
        'user': tweet['user']['screen_name'],
        'followers_count': data['user']['followers_count'],
        'friends_count': data['user']['friends_count']}


class ExtractRetweets(object):
    def __init__(self, infile, indir, outfile):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.outfile = outfile

        self.n_tweets = 0
        self.n_retweets = 0
        self.n_inretweets = 0
        self.n_quotes = 0
        self.n_inquotes = 0

        self.tweets = {}
        self.retweets = defaultdict(list)
        self.quotes = defaultdict(list)

    def _user_path(self, user_id):
        return os.path.join(self.indir, str(user_id))

    def _user_files(self, user_id):
        # TEMPORARY HACK!!! (DATE)
        file_names = glob.glob(
            os.path.join(self._user_path(user_id), '2020-07.json.gz'))
        return file_names

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
                                ruser = tweet['retweeted_status']['user']
                                ruid = ruser['id']
                                if ruid in self.user_ids:
                                    self.n_inretweets += 1
                                usr = ruser['scree_name']
                                parent = tweet['retweeted_status']
                                parent_id = parent['id_str']
                                self.retweets[parent_id].append(usr)
                                self.tweets[parent_id] = simple(parent)
                            elif 'quoted_status' in tweet:
                                self.n_quotes += 1
                                ruid = tweet['quoted_status']['user']['id']
                                if ruid in self.user_ids:
                                    self.n_inquotes += 1
                                parent = tweet['quoted_status']
                                parent_id = parent['id_str']
                                self.quotes[parent_id].append(simple(tweet))
                                self.tweets[parent_id] = simple(parent)

                fileds = ['tweets',
                          'retweets', 'inretweets',
                          'quotes', 'inquotes']
                field_strs = ['# {}: {{}}'.format(filed) for field in fields]
                info_str = '; '.join(field_strs)
                print(info_str.format(self.n_tweets,
                                      self.n_retweets, self.n_inretweets,
                                      self.n_quotes, self.n_inquotes))

        with open(self.outfile, 'wt') as f:
            for tid in self.quotes:
                if len(self.quotes[tid]) > 0:
                    tweet = self.tweets[tid]
                    tweet['retweets'] = self.retweets[tid]
                    tweet['quotes'] = self.quotes[tid]
                    f.write('{}\n'.format(json.dumps(tweet)))


if __name__ == '__main__':
    tr = ExtractRetweets(
        'retweet-userids.csv', 'timelines', 'retweets.json')
    tr.run()
