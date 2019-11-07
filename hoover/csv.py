import csv
import json


FIELDS = ('created_at',
          'id',
          'text',
          'full_text',
          'retweet_count',
          'favorite_count',
          'lang',
          'user_id',
          'user_screen_name',
          'retweeted',
          'retweeted_id',
          'retweeted_user_id',
          'retweeted_user_screen_name',
          'quoted',
          'quoted_id',
          'quoted_user_id',
          'quoted_user_screen_name')


def to_csv(infile, outfile):
    with open(infile, 'r') as infile, open(outfile, 'w') as outfile:

        csvwriter = csv.writer(outfile)
        csvwriter.writerow(FIELDS)

        for line in infile:
            data = json.loads(line)
            simple_data = {
                'created_at': data['created_at'],
                'id': data['id_str'],
                'text': data['text'],
                'full_text': data['text'],
                'retweet_count': data['retweet_count'],
                'favorite_count': data['favorite_count'],
                'lang': data['lang'],
                'user_id': data['user']['id_str'],
                'user_screen_name': data['user']['screen_name'],
                'user_followers_count': data['user']['followers_count'],
                'user_friends_count': data['user']['friends_count'],
                'user_lang': data['user']['lang'],
                'retweeted': False,
                'retweeted_id': None,
                'retweeted_user_id': None,
                'retweeted_user_screen_name': None,
                'quoted': False,
                'quoted_id': None,
                'quoted_user_id': None,
                'quoted_user_screen_name': None
            }

            if 'extended_tweet' in data:
                simple_data['full_text'] = data['extended_tweet']['full_text']

            if 'retweeted_status' in data:
                rs = data['retweeted_status']
                simple_data['retweeted'] = True
                simple_data['retweeted_id'] = rs['id_str']
                simple_data['retweeted_user_id'] = rs['user']['id_str']
                simple_data['retweeted_user_screen_name'] \
                    = rs['user']['screen_name']
                if 'extended_tweet' in rs:
                    simple_data['full_text'] \
                        = rs['extended_tweet']['full_text']

            if 'quoted_status' in data:
                qs = data['quoted_status']
                simple_data['quoted'] = True
                simple_data['quoted_id'] = qs['id_str']
                simple_data['quoted_user_id'] = qs['user']['id_str']
                simple_data['quoted_user_screen_name'] \
                    = qs['user']['screen_name']
                if 'extended_tweet' in qs:
                    simple_data['full_text'] \
                        = qs['extended_tweet']['full_text']

            csvwriter.writerow([simple_data[field] for field in FIELDS])
