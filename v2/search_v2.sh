#!/bin/bash

KEYS_FOLDER_NAME=$1
KEYWORDS_PATH=$2
LANG=$3 # either language code or 'all' if collection in all possible langauges
START_TIME=$4 # YYYY-MM-DD format
END_TIME=$5 # YYYY-MM-DD format. If collection until present time, give value 'none'
OUTFILE=$6
ANONYMIZE=$7

source /home/socsemics/code/envs/socsemics_env/bin/activate


python /home/socsemics/code/twitter-hoover/v2/search_v2.py \
--keys_folder_name ${KEYS_FOLDER_NAME} \
--lang ${LANG} \
--keywords_path ${KEYWORDS_PATH} \
--start_time ${START_TIME} \
--end_time ${END_TIME} \
--outfile ${OUTFILE} \
--anonymize ${ANONYMIZE}

