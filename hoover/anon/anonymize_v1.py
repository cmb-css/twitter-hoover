from __future__ import absolute_import
import string
import itertools
import argparse
import pandas as pd
import os
import logging
from hoover.anon.utils import load_key_to_decrypt_anon, decrypt_anon, kept_anonymized_tweet_objects_list, \
    kept_tweet_objects_list, removed_tweet_objects_list, kept_anonymized_user_objects_list, kept_user_objects_list, \
    removed_user_objects_list, kept_anonymized_entities_dict, kept_entities_list, removed_entities_list, \
    determine_id_type, \
    save_to_json
import base64
import ast
import re
from pathlib import Path
import gzip
import hashlib
import json
from Crypto.Cipher import AES
from base64 import b64encode, b64decode
import pickle
import time
import shutil
import sys

logging.basicConfig(filename=f'/home/data/socsemics/code/twitter-hoover/hoover/anon/logs/{int(time.time())}.txt',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, help="Path to the file to be encrypted.")
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.", default='/home/socsemics/anon')
    # parser.add_argument("--compressed", type=str, help="Whether the files are compressed or not.")
    parser.add_argument("--data_type", type=str, help="Type of collected data.")
    parser.add_argument("--resume", type=int, help="Whether to resume or not", default=0)
    parser.add_argument("--most_recent", type=str, default='all')
    args = parser.parse_args()
    return args


def retrieve_key_from_anon(hash_range_str, anon_dict):
    if hash_range_str in anon_dict:
        return anon_dict[hash_range_str]
        # assert anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].shape[0] == 1
        # key = anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].iloc[0]
        # return key


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
    return [b64encode(x).decode('utf-8') for x in [ciphertext, tag]]


def isascii(s):
    try:
        s.encode('ascii')
    except UnicodeEncodeError:
        return False
    else:
        return True


def anonymize_raw(id, id_type, anon_dict, social_network='T'):
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
    if id is None:
        return None
    if not isascii(id):
        id = id.encode("ascii", "ignore").decode()
    hashed_id = hash_encode(id=id)
    if hashed_id:
        hash_range_str = hashed_id[:3].decode()
        key = retrieve_key_from_anon(hash_range_str=hash_range_str, anon_dict=anon_dict)
        ciphertext, tag = aes_siv_encrypt(key=key, data=id.encode("utf-8"))
        anonymized_id = f'{id_type}.{social_network}.{hash_range_str}.{ciphertext}.{tag}'
        return anonymized_id.replace('/', '*')
    else:
        logger.info(f'ID {id} cannot be encoded')


def anonymize(data_dict, dict_key, object_type, anon_dict, social_network='T'):
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
    return anonymize_raw(id, id_type, anon_dict, social_network=social_network)


def clean_anonymize_user(line_dict, output_dict, anon_dict):
    output_dict['user'] = dict()
    for user_key in line_dict['user'].keys():
        if user_key in kept_anonymized_user_objects_list:
            if line_dict["user"][user_key]:
                output_dict['user'][user_key] = anonymize(data_dict=line_dict['user'], dict_key=user_key,
                                                          object_type='user', anon_dict=anon_dict)
            else:
                output_dict['user'][user_key] = ''
        elif user_key in kept_user_objects_list:
            output_dict['user'][user_key] = line_dict['user'][user_key]
        elif user_key == 'url':
            if line_dict["user"][user_key]:
                output_dict['user'][user_key] = anonymize(data_dict=line_dict['user'], dict_key=user_key,
                                                          object_type='urls', anon_dict=anon_dict)
            else:
                output_dict['user'][user_key] = ''
        elif user_key == 'description':
            if line_dict["user"][user_key]:
                output_dict['user'][user_key] = anonymize_text(line_dict["user"][user_key], anon_dict=anon_dict)
            else:
                output_dict['user'][user_key] = ''
    return output_dict


def clean_anonymize_retweeted_status(line_dict, output_dict, anon_dict):
    output_dict['retweeted_status'] = dict()
    for key in line_dict['retweeted_status'].keys():
        if key == 'user':
            output_dict['retweeted_status'] = clean_anonymize_user(line_dict=line_dict['retweeted_status'],
                                                                   output_dict=output_dict['retweeted_status'],
                                                                   anon_dict=anon_dict)
        elif key == 'url':
            if line_dict['retweeted_status'][key]:
                output_dict['retweeted_status'][key] = anonymize(data_dict=line_dict['retweeted_status'],
                                                                 dict_key=key, object_type='urls', anon_dict=anon_dict)
            else:
                output_dict['retweeted_status'][key] = ''
        elif key in kept_anonymized_tweet_objects_list:
            if line_dict['retweeted_status'][key]:
                output_dict['retweeted_status'][key] = anonymize(data_dict=line_dict['retweeted_status'],
                                                                 dict_key=key, object_type='tweet', anon_dict=anon_dict)
            else:
                output_dict['retweeted_status'][key] = ''
        elif key in kept_tweet_objects_list:
            output_dict['retweeted_status'][key] = line_dict['retweeted_status'][key]
    return output_dict


def clean_anonymize_quoted_status(line_dict, output_dict, anon_dict):
    output_dict['quoted_status'] = dict()
    for key in line_dict['quoted_status'].keys():
        if key == 'user':
            output_dict['quoted_status'] = clean_anonymize_user(line_dict=line_dict['quoted_status'],
                                                                output_dict=output_dict['quoted_status'],
                                                                anon_dict=anon_dict)
        elif key == 'url':
            if line_dict['quoted_status'][key]:
                output_dict['quoted_status'][key] = anonymize(data_dict=line_dict['quoted_status'],
                                                              dict_key=key, object_type='urls', anon_dict=anon_dict)
            else:
                output_dict['quoted_status'][key] = line_dict['quoted_status'][key]
        elif key in kept_anonymized_tweet_objects_list:
            if line_dict['quoted_status'][key]:
                output_dict['quoted_status'][key] = anonymize(data_dict=line_dict['quoted_status'],
                                                              dict_key=key, object_type='tweet', anon_dict=anon_dict)
            else:
                output_dict['quoted_status'][key] = line_dict['quoted_status'][key]
        elif key in kept_tweet_objects_list:
            output_dict['quoted_status'][key] = line_dict['quoted_status'][key]
    return output_dict


def clean_anonymize_entities(line_dict, output_dict, anon_dict):
    output_dict['entities'] = dict()
    for key in line_dict['entities'].keys():
        if key == 'user_mentions':
            user_mentions_list = list()
            for user_dict in line_dict['entities']['user_mentions']:
                anonymized_user_dict = dict()
                anonymized_user_dict['screen_name'] = anonymize(data_dict=user_dict, dict_key='screen_name',
                                                                object_type='user', anon_dict=anon_dict)
                anonymized_user_dict['id_str'] = anonymize(data_dict=user_dict, dict_key='id_str',
                                                           object_type='user', anon_dict=anon_dict)
                user_mentions_list.append(anonymized_user_dict)
            output_dict['entities']['user_mentions'] = user_mentions_list
        elif key == 'urls':
            urls_list = list()
            for url_dict in line_dict['entities']['urls']:
                anonymized_url_dict = dict()
                anonymized_url_dict['url'] = anonymize(data_dict=url_dict, dict_key='url', object_type='urls',
                                                       anon_dict=anon_dict)
                anonymized_url_dict['expanded_url'] = anonymize(data_dict=url_dict, dict_key='expanded_url',
                                                                object_type='urls', anon_dict=anon_dict)
                urls_list.append(anonymized_url_dict)
            output_dict['entities']['urls'] = urls_list
        elif key in kept_entities_list:
            output_dict['entities'][key] = line_dict['entities'][key]
    return output_dict


def anonymize_text(text, anon_dict):
    screen_name_list = [i[1:] for i in text.split() if i.startswith('@')]
    if text[:2] == 'RT' and len(screen_name_list) > 0:
        screen_name_list[0] = screen_name_list[0].replace(':', '')
    url_list = re.findall(r'(https?://\S+)', text)
    # build anonymized screen names and urls
    replace_dict = dict()
    for screen_name in screen_name_list:
        replace_dict[screen_name] = anonymize(data_dict=screen_name, dict_key='screen_name', object_type='text',
                                              anon_dict=anon_dict)
    for url in url_list:
        replace_dict[url] = anonymize(data_dict=url, dict_key='tweet_url', object_type='text', anon_dict=anon_dict)
    # replace screen names and urls in text with anonymized versions
    anonymized_text = text
    for to_replace_str in replace_dict.keys():
        if replace_dict[to_replace_str]:
            anonymized_text = anonymized_text.replace(to_replace_str, replace_dict[to_replace_str])
    return anonymized_text


def clean_anonymize_text(line_dict, output_dict, anon_dict):
    if 'full_text' in line_dict.keys():
        tweet = line_dict['full_text']
    elif 'text' in line_dict.keys() and not 'full_text' in line_dict.keys():
        tweet = line_dict['text']
    else:
        tweet = []
    output_dict['text'] = anonymize_text(tweet, anon_dict=anon_dict)
    return output_dict


def clean_anonymize_line_dict(line_dict, anon_dict):
    output_dict = dict()
    for key in line_dict.keys():
        if key == 'user':
            output_dict = clean_anonymize_user(line_dict=line_dict, output_dict=output_dict, anon_dict=anon_dict)
        elif key == 'retweeted_status':
            output_dict = clean_anonymize_retweeted_status(line_dict=line_dict, output_dict=output_dict,
                                                           anon_dict=anon_dict)
        elif key == 'quoted_status':
            output_dict = clean_anonymize_quoted_status(line_dict=line_dict, output_dict=output_dict,
                                                        anon_dict=anon_dict)
        elif key in ['text', 'full_text']:
            output_dict = clean_anonymize_text(line_dict=line_dict, output_dict=output_dict, anon_dict=anon_dict)
        elif key == 'entities':
            output_dict = clean_anonymize_entities(line_dict=line_dict, output_dict=output_dict, anon_dict=anon_dict)
        elif key in kept_anonymized_tweet_objects_list:
            if line_dict[key]:
                output_dict[key] = anonymize(data_dict=line_dict, dict_key=key, object_type='tweet',
                                             anon_dict=anon_dict)
            else:
                output_dict[key] = line_dict[key]
        elif key in kept_tweet_objects_list:
            output_dict[key] = line_dict[key]
    output_dict['anon'] = 1
    return output_dict


def clean_line(line):
    line = line.replace('false', 'False').replace('true', 'True').replace('null', 'None').replace('\n', '')
    line_split = line.split('"source":')
    final_list = list()
    # logger.info(line_split)
    for count, chunk in enumerate(line_split):
        if count > 0:
            chunk = chunk.split('",', 1)[1]
        final_list.append(chunk)
    clean_line = ''.join(final_list)
    if not isascii(clean_line):
        clean_line = clean_line.encode("ascii", "ignore").decode()
    if r'\N' in fr'{clean_line}':
        # logger.info('Found backslash capital n, replacing it.')
        # logger.info(fr'Line before replacement: {line}')
        clean_line = fr'{clean_line}'.replace(r'\N', '\ N')
        # logger.info(fr'Line after replacement: {line}')
    if '}{"created_at"' in clean_line:
        clean_line_split = clean_line.split('}{"created_at"')
        clean_line_split[0] = f'{clean_line_split[0]}' + '}'
        for i in range(1, len(clean_line_split)):
            clean_line_split[i] = '{' + f'"created_at"{clean_line_split[i]}'
        return clean_line_split
    else:
        return [clean_line]


def use_input_path_to_define_output(input_path, output_folder):
    filename = os.path.basename(input_path)
    return f'{output_folder}/{filename}'


def display_time(seconds, intervals, granularity=4):
    result = []
    for name, count in intervals:
        value = seconds // count
        if value:
            seconds -= value * count
            if value == 1:
                name = name.rstrip('s')
            result.append("{} {}".format(value, name))
    return ', '.join(result[:granularity])


def get_list_of_already_anon_users(log_path):
    with open(log_path) as file:
        lines = file.readlines()
    user_id_list = list()
    for line in lines:
        user_id_list.append(line.replace('\n', ''))
    return user_id_list


def save_id_of_anon_user(log_path, anon_id):
    with open(log_path, "a") as text_file:
        print(f'{anon_id}\n', file=text_file)


def keep_all_but_most_recent_folder(paths_to_encrypt_list):
    path_list = [path for path in paths_to_encrypt_list]
    filename_list = [path.name.split('.')[0] for path in path_list]
    if len(filename_list) > 1:
        logger.info('Data collected on more than one month')
        filename_dict = dict()
        for filename in filename_list:
            filename_dict[filename] = pd.to_datetime(filename, format='%Y-%m')
        most_recent_foldername = max(filename_dict, key=filename_dict.get)
        return [path for path in path_list if most_recent_foldername not in path.name], most_recent_foldername
    elif len(filename_list) == 1:
        logger.info('Data collected on only one month')
        return [], filename_list[0]


def keep_only_most_recent_folders(paths_to_encrypt_list, not_anon_file):
    path_list = [path for path in paths_to_encrypt_list]
    filename_list = [path.name.split('.')[0] for path in path_list]
    filename_dict = dict()
    for filename in filename_list:
        filename_dict[filename] = pd.to_datetime(filename, format='%Y-%m')
    former_most_recent = pd.to_datetime(not_anon_file.split('.')[0], format='%Y-%m')
    final_dict = dict((k, v) for k, v in filename_dict.items() if v >= former_most_recent)
    final_path_list = [path for path in path_list if any(s in path.name for s in final_dict.keys())]
    return final_path_list

def convert_dict_string_to_dict(cleaned_line):
    modified_cleaned_line = cleaned_line + '}'
    for line in [cleaned_line, modified_cleaned_line]:
        try:
            return ast.literal_eval(line)
            break
        except:
            pass
    logger.exception('Got exception on main handler')



if __name__ == '__main__':
    args = get_args_from_command_line()
    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)
    logger.info('Loaded anon DB')
    intervals = (
        ('weeks', 604800),  # 60 * 60 * 24 * 7
        ('days', 86400),  # 60 * 60 * 24
        ('hours', 3600),  # 60 * 60
        ('minutes', 60),
        ('seconds', 1),
    )
    if not os.path.exists(f"{args.input_path}_encrypted"):
        os.makedirs(f"{args.input_path}_encrypted", exist_ok=True)
    if args.resume == 1:
        already_anon_list = get_list_of_already_anon_users(log_path=f"{args.input_path}_encrypted/already_anon.log")
    else:
        open(f"{args.input_path}_encrypted/already_anon.log", 'w').close()
        already_anon_list = list()
    start_time = time.time()
    too_short_tweet_count = 0
    if args.data_type == 'timelines':
        if args.most_recent == 'only':
            not_anon_dict = dict()
            with open(f"{args.input_path}_encrypted/not_anon_files.json") as not_anon_f:
                for line in not_anon_f:
                    line_dict = ast.literal_eval(line)
                    not_anon_dict = {**not_anon_dict, **line_dict}
        if not os.path.exists(f'{args.input_path}_encrypted'):
            os.makedirs(f'{args.input_path}_encrypted', exist_ok=True)
        folder_list = os.listdir(args.input_path)
        total_count_tweet = 0
        for count, user_folder in enumerate(folder_list):
            if user_folder not in already_anon_list:
                logger.info(f'User #{count}/{len(folder_list)}')
                logger.info(f'Encrypting timeline from user {user_folder}')
                start_user = time.time()
                paths_to_encrypt_list = Path(os.path.join(args.input_path, user_folder)).glob('*.json.gz')
                anon_user_folder = anonymize(data_dict={'id_str': str(user_folder)}, dict_key='id_str',
                                             object_type='user', anon_dict=anon_dict)
                if os.path.exists(f'{args.input_path}_encrypted/{anon_user_folder}'):
                    logger.info(f'Output folder already exists. Deleting and recreating.')
                    shutil.rmtree(f'{args.input_path}_encrypted/{anon_user_folder}')
                    os.makedirs(f'{args.input_path}_encrypted/{anon_user_folder}')
                else:
                    os.makedirs(f'{args.input_path}_encrypted/{anon_user_folder}')
                count_tweet = 0
                if args.most_recent == 'not':
                    paths_to_encrypt_list, most_recent_foldername = keep_all_but_most_recent_folder(
                        paths_to_encrypt_list=paths_to_encrypt_list)
                    logger.info(f'Dropping {most_recent_foldername}')
                    username_to_most_recent_folder_dict = {user_folder: f'{most_recent_foldername}.json.gz'}
                    save_to_json(username_to_most_recent_folder_dict,
                                 f"{args.input_path}_encrypted/not_anon_files.json")
                elif args.most_recent == 'only':
                    not_anon_file = not_anon_dict[user_folder]
                    paths_to_encrypt_list = keep_only_most_recent_folders(
                        paths_to_encrypt_list=paths_to_encrypt_list, not_anon_file=not_anon_file)
                    logger.info(f'Encrypting only {paths_to_encrypt_list}')
                for path_to_encrypt in paths_to_encrypt_list:
                    logger.info(f'Encrypting {path_to_encrypt}')
                    path_to_encrypted = use_input_path_to_define_output(input_path=path_to_encrypt,
                                                                        output_folder=f'{args.input_path}_encrypted/{anon_user_folder}')
                    with gzip.open(path_to_encrypt, 'rt') as f:
                        with gzip.open(path_to_encrypted, 'wt') as out:
                            for line in f:
                                # logger.info(f'Raw line: {line}')
                                clean_line_split = clean_line(line=line)
                                for cleaned_line in clean_line_split:
                                    count_tweet += 1
                                    # logger.info(f'Cleaned line: {cleaned_line}')
                                    if len(cleaned_line) > 2:
                                        line_dict = convert_dict_string_to_dict(cleaned_line)
                                        if line_dict:
                                            if not 'anon' in line_dict.keys():
                                                output_dict = clean_anonymize_line_dict(line_dict=line_dict,
                                                                                        anon_dict=anon_dict)
                                                print(json.dumps(output_dict), file=out)
                                    else:
                                        too_short_tweet_count += 1
                current_time = time.time()
                logger.info(
                    f'Elapsed time for user {user_folder}: {display_time(seconds=current_time - start_user, intervals=intervals)}')
                logger.info(f'# anonymized tweets: {count_tweet}')
                if count_tweet > 0:
                    logger.info(f'Average anon time per tweet: {(current_time - start_user) / count_tweet}')
                logger.info(
                    f'Elapsed time since launch: {display_time(seconds=current_time - start_time, intervals=intervals)}')
                logger.info(f'Number of non-processed strings (shorter than 2 characters): {too_short_tweet_count}')
                total_count_tweet = + count_tweet
                save_id_of_anon_user(log_path=f"{args.input_path}_encrypted/already_anon.log", anon_id=user_folder)
                logger.info('*************************************')
            else:
                logger.info(f'User {user_folder} already anonymized. Skipping')
        end_time = time.time()
        logger.info(f'Total duration: {display_time(seconds=end_time - start_time, intervals=intervals)}')
        logger.info(f'Total # of anon tweets: {total_count_tweet}')
        logger.info(f'# of users covered: {count}')

