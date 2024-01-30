from __future__ import absolute_import
import string
import itertools
import argparse
from cryptography.fernet import Fernet
import pandas as pd
import os
import logging
from hoover.anon.utils import load_key_to_decrypt_anon, decrypt_anon, kept_anonymized_tweet_objects_list, \
    kept_tweet_objects_list, removed_tweet_objects_list, kept_anonymized_user_objects_list, kept_user_objects_list, \
    removed_user_objects_list, kept_anonymized_entities_dict, kept_entities_list, removed_entities_list, determine_id_type
import base64
import ast
import re
from pathlib import Path
import gzip
import hashlib
import json
from Crypto.Cipher import AES
from base64 import b64encode, b64decode

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, help="Path to the file to be encrypted.")
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.")
    # parser.add_argument("--compressed", type=str, help="Whether the files are compressed or not.")
    parser.add_argument("--data_type", type=str, help="Type of collected data.")
    args = parser.parse_args()
    return args


def retrieve_key_from_anon(hash_range_str, anon_db_folder_path):
    anon_path = os.path.join(anon_db_folder_path, 'anon-DB.csv')
    anon_df = pd.read_csv(anon_path, keep_default_na=False)
    if hash_range_str in anon_df['hash_range'].unique():
        assert anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].shape[0] == 1
        key = anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].iloc[0]
        return key


def hash_encode(id):
    try:
        assert isinstance(id, str)
        hasher = hashlib.sha256(id.encode())
        return base64.standard_b64encode(hasher.digest())
    except:
        return None

def aes_siv_encrypt(key, data):
    key = b64decode(key)
    cipher = AES.new(key, AES.MODE_SIV)
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return [ b64encode(x).decode('utf-8') for x in [ciphertext, tag] ]


def anonymize(data_dict, dict_key, object_type, anon_db_folder_path, social_network='T'):
    """Anonymize the selected id based on id type.
    Parameters:
        id (str): the id to anonymize
        id_type (str): the id type of `id`. Possible values are 'UID' for user ID, 'USN' for user name, 'TID' for tweet
        ID and 'TURL' for tweet URL.
        social_network (str): the online social network from which the data is extracted (T for Twitter as default)
    Returns:
         An anonymized ID in string format composed of four elements separated by dots: '<id_type>.<social_network>.<hash_range>.<encrypted_id>'
         where:
            - <hash_range>: the first 3 characters in the hash string
            - <encrypted_id>: the encrypted ID in UTF-8 format
    """
    id_type = determine_id_type(dict_key=dict_key, object_type=object_type)
    if object_type == 'text':
        id = data_dict
    else:
        id = data_dict[dict_key]
    hashed_id = hash_encode(id=id)
    if hashed_id:
        hash_range_str = hashed_id[:3].decode()
        key = retrieve_key_from_anon(hash_range_str=hash_range_str, anon_db_folder_path=anon_db_folder_path)
        ciphertext, tag = aes_siv_encrypt(key=key, data=id.encode("utf-8"))
        anonymized_id = f'{id_type}.{social_network}.{hash_range_str}.{ciphertext}.{tag}'
        return anonymized_id.replace('/', '*')
    else:
        logger.info(f'ID {id} cannot be encoded')


def clean_anonymize_user(line_dict, output_dict, anon_db_folder_path):
    output_dict['user'] = dict()
    # anon
    for user_key in kept_anonymized_user_objects_list:
        if user_key in line_dict['user'] and line_dict["user"][user_key]:
            output_dict['user'][user_key] = anonymize(data_dict=line_dict['user'], dict_key=user_key,
                                                      object_type='user', anon_db_folder_path=anon_db_folder_path)
        else:
            output_dict['user'][user_key] = ''
    # keep and not anon
    for user_key in kept_user_objects_list:
        if user_key in line_dict['user']:
            output_dict['user'][user_key] = line_dict['user'][user_key]
    #url
    if 'url' in line_dict['user'] and line_dict["user"]['url']:
        output_dict['user']['url'] = anonymize(data_dict=line_dict['user'], dict_key='url',
                                                  object_type='urls', anon_db_folder_path=anon_db_folder_path)
    # description
    if 'description' in line_dict['user'] and line_dict["user"]['description']:
        output_dict['user']['description'] = anonymize_text(line_dict["user"]['description'], anon_db_folder_path=anon_db_folder_path)
    return output_dict


def clean_anonymize_retweeted_status(line_dict, output_dict, anon_db_folder_path):
    output_dict['retweeted_status'] = dict()
    # anon
    for key in kept_anonymized_tweet_objects_list:
        if key in line_dict['retweeted_status'] and line_dict['retweeted_status'][key]:
            output_dict['retweeted_status'][key] = anonymize(data_dict=line_dict['retweeted_status'],
                                                             dict_key=key, object_type='tweet',
                                                             anon_db_folder_path=anon_db_folder_path)
    # kept but not anon
    for key in kept_tweet_objects_list:
        if key in line_dict['retweeted_status']:
            output_dict['retweeted_status'][key] = line_dict['retweeted_status'][key]
    # user
    if 'user' in line_dict['retweeted_status']:
        output_dict['retweeted_status'] = clean_anonymize_user(line_dict=line_dict['retweeted_status'],
                                                               output_dict=output_dict['retweeted_status'], anon_db_folder_path=anon_db_folder_path)
    # url
    if 'url' in line_dict['retweeted_status'] and line_dict['retweeted_status']['url']:
        output_dict['retweeted_status']['url'] = anonymize(data_dict=line_dict['retweeted_status'],
                                                         dict_key='url', object_type='urls', anon_db_folder_path=anon_db_folder_path)
    return output_dict

def clean_anonymize_quoted_status(line_dict, output_dict, anon_db_folder_path):
    output_dict['quoted_status'] = dict()
    # anon
    for key in kept_anonymized_tweet_objects_list:
        if key in line_dict['quoted_status']:
            output_dict['quoted_status'][key] = anonymize(data_dict=line_dict['quoted_status'],
                                                          dict_key=key, object_type='tweet',
                                                          anon_db_folder_path=anon_db_folder_path)
    # kept but non anon
    for key in kept_tweet_objects_list:
        if key in line_dict['quoted_status']:
            output_dict['quoted_status'][key] = line_dict['quoted_status'][key]
    # user
    if 'user' in line_dict['quoted_status']:
        output_dict['quoted_status'] = clean_anonymize_user(line_dict=line_dict['quoted_status'],
                                                               output_dict=output_dict['quoted_status'], anon_db_folder_path=anon_db_folder_path)
    # url
    if 'url' in line_dict['quoted_status'] and line_dict['quoted_status']['url']:
        output_dict['quoted_status']['url'] = anonymize(data_dict=line_dict['quoted_status'],
                                                         dict_key='url', object_type='urls', anon_db_folder_path=anon_db_folder_path)
    return output_dict

def clean_anonymize_entities(line_dict, output_dict, anon_db_folder_path):
    output_dict['entities'] = dict()
    # kept entities
    for key in kept_entities_list:
        if key in line_dict['entities']:
            output_dict['entities'][key] = line_dict['entities'][key]
    # user_mentions
    if 'user_mentions' in line_dict['entities']:
        user_mentions_list = list()
        for user_dict in line_dict['entities']['user_mentions']:
            anonymized_user_dict = dict()
            anonymized_user_dict['screen_name'] = anonymize(data_dict=user_dict, dict_key='screen_name', object_type='user', anon_db_folder_path=anon_db_folder_path)
            anonymized_user_dict['id_str'] = anonymize(data_dict=user_dict, dict_key='id_str',
                                                            object_type='user', anon_db_folder_path=anon_db_folder_path)
            user_mentions_list.append(anonymized_user_dict)
        output_dict['entities']['user_mentions'] = user_mentions_list
    # urls
    if 'urls' in line_dict['entities']:
        urls_list = list()
        for url_dict in line_dict['entities']['urls']:
            anonymized_url_dict = dict()
            anonymized_url_dict['url'] = anonymize(data_dict=url_dict, dict_key='url', object_type='urls', anon_db_folder_path=anon_db_folder_path)
            anonymized_url_dict['expanded_url'] = anonymize(data_dict=url_dict, dict_key='expanded_url',
                                                            object_type='urls', anon_db_folder_path=anon_db_folder_path)
            urls_list.append(anonymized_url_dict)
        output_dict['entities']['urls'] = urls_list
    return output_dict

def anonymize_text(text, anon_db_folder_path):
    screen_name_list = [i[1:] for i in text.split() if i.startswith('@')]
    if text[:2] == 'RT':
        screen_name_list[0] = screen_name_list[0].replace(':', '')
    url_list = re.findall(r'(https?://\S+)', text)
    # build anonymized screen names and urls
    replace_dict = dict()
    for screen_name in screen_name_list:
        replace_dict[screen_name] = anonymize(data_dict=screen_name, dict_key='screen_name', object_type='text', anon_db_folder_path=anon_db_folder_path)
    for url in url_list:
        replace_dict[url] = anonymize(data_dict=url, dict_key='tweet_url', object_type='text', anon_db_folder_path=anon_db_folder_path)
    # replace screen names and urls in text with anonymized versions
    anonymized_text = text
    for to_replace_str in replace_dict.keys():
        anonymized_text = anonymized_text.replace(to_replace_str, replace_dict[to_replace_str])
    return anonymized_text

def clean_anonymize_text(line_dict, output_dict, anon_db_folder_path):
    if 'full_text' in line_dict:
        tweet = line_dict['full_text']
    elif 'text' in line_dict and not 'full_text' in line_dict:
        tweet = line_dict['text']
    else:
        tweet = []
    output_dict['text'] = anonymize_text(tweet, anon_db_folder_path=anon_db_folder_path)
    return output_dict


def clean_anonymize_line_dict(line_dict, anon_db_folder_path):
    output_dict = dict()
    for key in line_dict.keys():
        if key == 'user':
            output_dict = clean_anonymize_user(line_dict=line_dict, output_dict=output_dict, anon_db_folder_path=anon_db_folder_path)
        elif key == 'retweeted_status':
            output_dict = clean_anonymize_retweeted_status(line_dict=line_dict, output_dict=output_dict, anon_db_folder_path=anon_db_folder_path)
        elif key == 'quoted_status':
            output_dict = clean_anonymize_quoted_status(line_dict=line_dict, output_dict=output_dict, anon_db_folder_path=anon_db_folder_path)
        elif key in ['text', 'full_text']:
            output_dict = clean_anonymize_text(line_dict=line_dict, output_dict=output_dict, anon_db_folder_path=anon_db_folder_path)
        elif key == 'entities':
            output_dict = clean_anonymize_entities(line_dict=line_dict, output_dict=output_dict, anon_db_folder_path=anon_db_folder_path)
        elif key in kept_anonymized_tweet_objects_list:
            if line_dict[key]:
                output_dict[key] = anonymize(data_dict=line_dict, dict_key=key, object_type='tweet', anon_db_folder_path=anon_db_folder_path)
            else:
                output_dict[key] = line_dict[key]
        elif key in kept_tweet_objects_list:
            output_dict[key] = line_dict[key]
    output_dict['anon'] = 1
    return output_dict


def use_input_path_to_define_output(input_path):
    user_folder_path = os.path.basename(os.path.dirname(input_path))
    filename = os.path.basename(input_path)
    return f'{args.input_path}_encrypted/{user_folder_path}/{filename}'

if __name__ == '__main__':
    args = get_args_from_command_line()
    if args.data_type == 'timelines':
        if not os.path.exists(f'{args.input_path}_encrypted'):
            os.makedirs(f'{args.input_path}_encrypted', exist_ok=True)
        folder_list = os.listdir(args.input_path)
        for user_folder in folder_list:
            paths_to_encrypt_list = Path(os.path.join(args.input_path, user_folder)).glob('*.json.gz')
            for path_to_encrypt in paths_to_encrypt_list:
                path_to_encrypted = use_input_path_to_define_output(input_path=path_to_encrypt)
                if not os.path.exists(os.path.dirname(path_to_encrypted)):
                    os.makedirs(os.path.dirname(path_to_encrypted), exist_ok=True)
                with gzip.open(path_to_encrypt, 'rt') as f:
                    with gzip.open(path_to_encrypted, 'wt') as out:
                        for line in f:
                            line = line.replace('false', 'False').replace('true', 'True').replace('null', 'None')
                            # try:
                            line_dict = ast.literal_eval(line)
                            if not 'anon' in line_dict.keys():
                                output_dict = clean_anonymize_line_dict(line_dict=line_dict, anon_db_folder_path=args.anon_db_folder_path)
                                print(json.dumps(output_dict), file=out)