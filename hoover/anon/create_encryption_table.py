import string
import itertools
import argparse
from cryptography.fernet import Fernet
import pandas as pd
import os
import logging
from utils import load_key_to_decrypt_anon
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
from base64 import b64encode
import pickle

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_path", type=str, help="Name of the folder where keys are stored")
    args = parser.parse_args()
    return args


def generate_all_possible_hash_ranges(length=3):
    chars = string.ascii_lowercase + string.ascii_uppercase + string.digits + '/=+'
    all_possible_hash_ranges_list = list()
    for item in itertools.product(chars, repeat=length):
        assert len(''.join(item)) == 3
        all_possible_hash_ranges_list.append(''.join(item))
    return all_possible_hash_ranges_list


def write_key(output_path):
    """Generate a key and save it into a file."""
    key = get_random_bytes(32)
    with open(os.path.join(output_path, "anon-DB.key"), "wb") as key_file:
        key_file.write(key)

def encrypt_anon(original_csv_path, encrypted_csv_path, key):
    """ Given a filename (str) and key (bytes), encrypt the file and write it."""
    cipher = AES.new(key, AES.MODE_SIV)
    with open(original_csv_path, "rb") as file:
        # read all file data
        file_data = file.read()
    # encrypt data
    encrypted_data, tag = cipher.encrypt_and_digest(file_data)
    # write the encrypted file
    with open(encrypted_csv_path, "wb") as file:
        file.write(b64encode(encrypted_data).decode('utf-8'))


if __name__ == '__main__':
    args = get_args_from_command_line()
    all_possible_hash_ranges_list = generate_all_possible_hash_ranges()
    range_to_key_dict = dict()
    for count, hash_range in enumerate(all_possible_hash_ranges_list):
        if hash_range:
            encryption_key = b64encode(get_random_bytes(32)).decode('utf-8')
            # while encryption_key in range_to_key_dict.values():
            #     encryption_key = get_random_bytes(32)
            range_to_key_dict[hash_range] = encryption_key
    encryption_df = pd.DataFrame(range_to_key_dict.items(), columns=['hash_range', 'encryption_key'])
    assert len(encryption_df['hash_range']) == encryption_df.shape[0]
    assert len(encryption_df['encryption_key']) == encryption_df.shape[0]
    anon_DB_path = os.path.join(args.output_path, 'anon-DB.pickle')
    with open(anon_DB_path, 'wb') as handle:
        pickle.dump(range_to_key_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    # encryption_df.to_csv(anon_DB_path, index=False)
    # encrypt the anon-DB
    # write_key(output_path=args.output_path)
    # key = load_key_to_decrypt_anon(anon_path=args.output_path)
    # logger.info(f'Key to encode anon-DB: {key}')
    # encrypt_anon(original_csv_path=anon_DB_path, encrypted_csv_path=anon_DB_path, key=key)
    # decrypt_anon(encrypted_csv_path=anon_DB_path, decrypted_csv_path=os.path.join(args.output_path,'anon-DB-decrypted.csv'), key=key)