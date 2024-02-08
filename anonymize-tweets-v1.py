import argparse
import json
import os
import pickle

from hoover.anon.anonymize_v1 import clean_line, convert_dict_string_to_dict, clean_anonymize_line_dict


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, help='Input file.')
    parser.add_argument('--outfile', type=str, help='Output file.')
    parser.add_argument('--anon_db_folder_path', type=str, help='Path to anon DB.', default='/home/socsemics/anon')
    args = parser.parse_args()

    anon_path = os.path.join(args.anon_db_folder_path, 'anon-DB.pickle')
    with open(anon_path, 'rb') as handle:
        anon_dict = pickle.load(handle)

    infile = args.infile
    outfile = args.outfile

    print('infile: {}'.format(infile))
    print('outfile: {}'.format(outfile))

    n = 0
    with open(infile, 'rt') as f:
        with open(outfile, 'wt') as out:
            for line in f:
                clean_line_split = clean_line(line=line)
                for cleaned_line in clean_line_split:
                    n += 1
                    if len(cleaned_line) > 2:
                        line_dict = convert_dict_string_to_dict(cleaned_line)
                        if line_dict:
                            if not 'anon' in line_dict.keys():
                                output_dict = clean_anonymize_line_dict(line_dict=line_dict, anon_dict=anon_dict)
                                print(json.dumps(output_dict), file=out)

    print(f'{n} tweets anonymized.')
