import json
from hoover.snowflake import str2utc


def read_simple(infile):
    tweets = []
    with open(infile, 'r') as file:
        for line in file:
            data = json.loads(line)
            # print(data)
            # print('*' * 100)
            simple_data = {
                'created_at': data['created_at'],
                'timestamp': str2utc(data['created_at']),
                'id': data['id_str'],
                'text': data['text'],
                'quoted_text': None,
                'retweet_count': data['retweet_count'],
                'favorite_count': data['favorite_count'],
                'lang': data['lang'],
                'reply': data['in_reply_to_status_id'] is not None,
                'in_reply_to_status_id': data['in_reply_to_status_id'],
                'in_reply_to_user_id': data['in_reply_to_user_id'],
                'in_reply_to_screen_name': data['in_reply_to_screen_name'],
                'user_id': data['user']['id_str'],
                'user_screen_name': data['user']['screen_name'],
                'user_followers_count': data['user']['followers_count'],
                'user_friends_count': data['user']['friends_count'],
                'user_lang': data['user']['lang'],
                'retweet': False,
                'retweeted_id': None,
                'retweeted_user_id': None,
                'retweeted_user_screen_name': None,
                'quote': False,
                'quoted_id': None,
                'quoted_user_id': None,
                'quoted_user_screen_name': None,
                'hashtags': tuple(item['text']
                                  for item
                                  in data['entities']['hashtags']),
                'mentions': tuple((item['id'], item['screen_name'])
                                  for item
                                  in data['entities']['user_mentions'])
            }

            if 'extended_tweet' in data:
                simple_data['text'] = data['extended_tweet']['full_text']

            if 'retweeted_status' in data:
                rs = data['retweeted_status']
                simple_data['retweet'] = True
                simple_data['retweeted_id'] = rs['id_str']
                simple_data['retweeted_user_id'] = rs['user']['id_str']
                simple_data['retweeted_user_screen_name'] \
                    = rs['user']['screen_name']
                simple_data['quoted_text'] = rs['text']
                if 'extended_tweet' in rs:
                    simple_data['quoted_text'] \
                        = rs['extended_tweet']['full_text']

            if 'quoted_status' in data:
                qs = data['quoted_status']
                simple_data['quote'] = True
                simple_data['quoted_id'] = qs['id_str']
                simple_data['quoted_user_id'] = qs['user']['id_str']
                simple_data['quoted_user_screen_name'] \
                    = qs['user']['screen_name']
                simple_data['quoted_text'] = qs['text']
                if 'extended_tweet' in qs:
                    simple_data['quoted_text'] \
                        = qs['extended_tweet']['full_text']

            tweets.append(simple_data)
        return tweets


def simplify(infile):
    for item in read_simple(infile):
        print(json.dumps(item))
