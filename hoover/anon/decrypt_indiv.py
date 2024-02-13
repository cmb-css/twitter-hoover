import string
import itertools
import argparse
from cryptography.fernet import Fernet
import pandas as pd
import os
import logging
from hoover.anon.utils import load_key_to_decrypt_anon, decrypt_anon, kept_anonymized_tweet_objects_list, \
    kept_tweet_objects_list, removed_tweet_objects_list, kept_anonymized_user_objects_list, kept_user_objects_list, \
    removed_user_objects_list, kept_anonymized_entities_dict, kept_entities_list, removed_entities_list
import base64
import ast
import re
from pathlib import Path
import gzip
import hashlib
from base64 import b64encode, b64decode
from Crypto.Cipher import AES


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--anonymized_id", type=str, help="Path to the ID to decrypt")
    parser.add_argument("--anon_db_folder_path", type=str, help="Path to anon DB.")

    args = parser.parse_args()
    return args

def retrieve_key_from_anon(hash_range_str, anon_db_folder_path):
    anon_path = os.path.join(anon_db_folder_path, 'anon-DB.csv')
    anon_df = pd.read_csv(anon_path, keep_default_na=False)
    if hash_range_str in anon_df['hash_range'].unique():
        assert anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].shape[0] == 1
        key = anon_df.loc[anon_df['hash_range'] == hash_range_str, 'encryption_key'].iloc[0]
        return key

def aes_siv_decrypt(key, ciphertext, tag):
    key = b64decode(key)
    cipher = AES.new(key, AES.MODE_SIV)
    return str(cipher.decrypt_and_verify(b64decode(ciphertext), b64decode(tag)).decode('utf-8'))

def deanonymize(anonymized_id, anon_db_folder_path):
    """Deanonymize the selected anonymized ID.
    Parameters:
        anonymized_id (str): the anonymized ID to deanonymize
    Returns:
        The original non encrypted ID, in string format
    """
    anonymized_id = anonymized_id.replace('*', '/')
    id_type, social_network, hash_range_str, ciphertext, tag = anonymized_id.split('.')
    key = retrieve_key_from_anon(hash_range_str=hash_range_str, anon_db_folder_path=anon_db_folder_path)
    return aes_siv_decrypt(key=key, ciphertext=ciphertext, tag=tag)

if __name__ == '__main__':
    args = get_args_from_command_line()
    # Decrypt anon DB
    # anon_key = load_key_to_decrypt_anon(anon_path=ANON_DB_FOLDER_PATH)
    # encrypted_anon_path = os.path.join(ANON_DB_FOLDER_PATH, 'anon-DB.csv')
    # decrypted_anon_path = os.path.join(ANON_DB_FOLDER_PATH, 'anon-DB-decrypted.csv')
    # decrypt_anon(encrypted_csv_path=encrypted_anon_path, decrypted_csv_path=decrypted_anon_path, key=anon_key)
    # Load anonymized data and deanonymize
    logger.info(f'Anonymized ID: {args.anonymized_id}')
    deanon_id = deanonymize(args.anonymized_id, anon_db_folder_path=args.anon_db_folder_path)
    logger.info(f'Deanonymized ID: {deanon_id}')
    # Save deanonymized data
    # output_path = f'{os.path.dirname(args.input_path)}/{os.path.basename(args.input_path).split(".")[0]}_deanonymized.csv'
