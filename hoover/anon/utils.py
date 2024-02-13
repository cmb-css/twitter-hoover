import os
import json
import hashlib
from cryptography.fernet import Fernet

API_KEYS_PATH = '/home/socsemics/data/api_keys/twitter'

# fields list for v1 cleaning and anonymization

kept_anonymized_tweet_objects_list = ['id_str',
                                      'in_reply_to_screen_name',
                                      'in_reply_to_status_id_str',
                                      'in_reply_to_user_id_str',
                                      'quoted_status_id_str']
                                      #'text']
                                      #'retweeted_status',
                                      #'user']

kept_tweet_objects_list = ['created_at',  # anonymize timestamp?
                                          'favorite_count',
                                          'lang',
                                          'possibly_sensitive',
                                          # This field only surfaces when a Tweet contains a link. The meaning of the field doesn’t pertain to the Tweet content itself, but instead it is an indicator that the URL contained in the Tweet may contain content or media identified as sensitive content.
                                          'retweet_count',
                                          'scopes',
                                          'truncated',
                                          'source']

removed_tweet_objects_list = ['id',  # tweet ID dropped because we already have it in str format
                              'in_reply_to_status_id',
                              # ID of tweet replied to, dropped because we already have it in str format
                              'in_reply_to_user_id',
                              # ID of user replied to, dropped because we already have it in str format
                              'annotations',
                              'contributors',
                              'coordinates',
                              'current_user_retweet',
                              'filter_level',
                              # Indicates the maximum value of the filter_level parameter which may be used and still stream this Tweet.
                              'geo',
                              'place',
                              'withheld_copyright',
                              'withheld_in_countries',
                              'withheld_scope']

kept_anonymized_user_objects_list = ['id_str', 'screen_name', 'profile_image_url_https']

kept_user_objects_list = ['created_at', 'followers_count', 'friends_count', 'statuses_count', 'verified', 'location'
                          ]

removed_user_objects_list = ['id',
                             'contributors_enabled',
                             'default_profile',
                             'default_profile_image',
                             'description',  # ?
                             'entities',  # entities in user description
                             'follow_request_sent',
                             'following',
                             'is_translator',
                             'name',  # ?
                             'notifications',
                             'profile_background_color',
                             'profile_background_image_url',
                             'profile_background_image_url_https',
                             'profile_background_tile',
                             'profile_banner_url',
                             'profile_image_url',
                             'profile_image_url_https',
                             'profile_link_color',
                             'profile_sidebar_border_color',
                             'profile_sidebar_fill_color',
                             'profile_text_color',
                             'profile_use_background_image',
                             'protected',
                             'show_all_inline_media',
                             'withheld_in_countries',
                             'withheld_scope',
                             'is_translation_enabled',
                             'utc_offset',
                             'timezone',
                             'geo_enabled',
                             'translator_type'
                             ]

kept_anonymized_entities_dict = {'user_mentions': ['screen_name', 'id_str'], 'urls': ['url', 'display_url', 'expanded_url']} #verify urls

kept_entities_list = ['hashtags']

removed_entities_list = ['media', 'symbols', 'polls']




def retrieve_keys(keys_folder_name):
    keys_file = open(os.path.join(API_KEYS_PATH, keys_folder_name, 'key-secret-token.txt'), 'r')
    return keys_file.read().splitlines()


def retrieve_keywords(keywords_path):
    keywords_file = open(keywords_path, 'r')
    return keywords_file.read().splitlines()


def build_search_query_keywords(keywords_path, lang):
    keywords_list = retrieve_keywords(keywords_path)
    query_str = '('
    for count, keyword in enumerate(keywords_list):
        if not keyword[:1] == '#':
            keyword = f'"{keyword}"'
        if count < len(keywords_list) - 1:
            query_str = f'{query_str}{keyword} OR '
        elif count == len(keywords_list) - 1:
            query_str = f'{query_str}{keyword})'
    if lang != 'all':
        query_str = f'{query_str} lang:{lang}'
    return query_str



def save_to_json(data_dict, outfile):
    with open(outfile, 'a') as file:
        file.write('{}\n'.format(json.dumps(data_dict)))


def load_key_to_decrypt_anon(anon_path):
    """ Load the string containing the key from the key file."""
    return open(os.path.join(anon_path, "anon-DB.key"), "rt").read()


def decrypt_anon(encrypted_csv_path, decrypted_csv_path, key):
    """ Given a filename (str) and key (bytes), decrypt the file and write it."""
    f = Fernet(key)
    with open(encrypted_csv_path, "rb") as file:
        # read the encrypted data
        encrypted_data = file.read()
    # decrypt data
    decrypted_data = f.decrypt(encrypted_data)
    # write the original file
    with open(decrypted_csv_path, "wb") as file:
        file.write(decrypted_data)

def determine_id_type(dict_key, object_type):
    if object_type == 'tweet':
        if dict_key in ['id_str', 'in_reply_to_status_id_str', 'quoted_status_id_str', 'id']:
            return 'TID'
        elif dict_key in ['in_reply_to_screen_name']:
            return 'USN'
        elif dict_key in ['in_reply_to_user_id_str', 'in_reply_to_user_id']:
            return 'UID'
    elif object_type == 'user':
        if dict_key in ['id_str', 'author_id', 'id']:
            return 'UID'
        elif dict_key in ['screen_name', 'username']:
            return 'USN'
        elif dict_key in ['profile_image_url_https']:
            return 'PPURL'
        elif dict_key in ['url']:
            return 'PWURL' #personal website URL
    elif object_type == 'text':
        if dict_key == 'screen_name':
            return 'USN'
        elif dict_key == 'tweet_url':
            return 'TURL'
    elif object_type == 'urls':
        return 'URL'

