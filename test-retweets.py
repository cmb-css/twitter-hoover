# coding=utf-8

import os
import glob
import gzip
import json
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


class TestRetweets(object):
    def __init__(self, infile, indir):
        self.user_ids = get_user_ids(infile)
        self.indir = indir
        self.tweets = 0
        self.retweets = 0
        self.inretweets = 0

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
                            self.tweets += 1
                            if 'retweeted_status' in tweet:
                                self.retweets += 1
                                ruid = tweet['retweeted_status']['user']['id']
                                if ruid in self.user_ids:
                                    self.inretweets += 1
                print('# tweets: {}; # retweets: {}; # inretweets: {}'.format(
                    self.tweets, self.retweets, self.inretweets))
                print('inretweet ratio: {}'.format(
                    float(self.inretweets) / float(self.retweets)))


if __name__ == '__main__':
    tr = TestRetweets('eu-elections-userids.csv', 'timelines')
    tr.run()
