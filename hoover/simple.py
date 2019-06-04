import json


def simplify(infile):
    with open(infile, 'r') as file:
        for line in file:
            data = json.loads(line)
            simple_data = {
                'created_at': data['created_at'],
                'id': data['id_str'],
                'text': data['text'],
                'full_text': data['text'],
                'quote_count': data['quote_count'],
                'reply_count': data['reply_count'],
                'retweet_count': data['retweet_count'],
                'favorite_count': data['favorite_count'],
                'lang': data['lang'],
                'timestamp_ms': data['timestamp_ms'],
                'user_id': data['user']['id_str'],
                'user_followers_count': data['user']['followers_count'],
                'user_friends_count': data['user']['friends_count'],
                'user_lang': data['user']['lang'],
                'retweeted': False,
                'retweeted_id': None,
                'retweeted_user_id': None,
                'quoted': False,
                'quoted_id': None,
                'quoted_user_id': None
            }

            if 'extended_tweet' in data:
                simple_data['full_text'] = data['extended_tweet']['full_text']

            if 'retweeted_status' in data:
                rs = data['retweeted_status']
                simple_data['retweeted'] = True
                simple_data['retweeted_id'] = rs['id_str']
                simple_data['retweeted_user_id'] = rs['user']['id_str']
                if 'extended_tweet' in rs:
                    simple_data['full_text'] \
                        = rs['extended_tweet']['full_text']

            if 'quoted_status' in data:
                qs = data['quoted_status']
                simple_data['quoted'] = True
                simple_data['quoted_id'] = qs['id_str']
                simple_data['quoted_user_id'] = qs['user']['id_str']
                if 'extended_tweet' in qs:
                    simple_data['full_text'] \
                        = qs['extended_tweet']['full_text']

            print(json.dumps(simple_data))
