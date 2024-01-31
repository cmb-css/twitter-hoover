import tweepy
from hoover.anon.utils import retrieve_keys, build_search_query_keywords, save_to_json
import argparse
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args_from_command_line():
    """Parse the command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--keys_folder_name", type=str, help="Name of the folder where keys are stored")
    parser.add_argument("--count_method", type=str,
                        help="Whether to get count for the full archive or the recent tweets. Two possibles values are 'full_archive' or 'recent'.")
    parser.add_argument("--lang", type=str,
                        help="Language of the tweets to be collected. If not provided, will collect tweets matching the query in all languages.")
    parser.add_argument("--keywords_path", type=str)
    parser.add_argument("--start_time", type=str, help="For full archive count, when to start the count. Format is YYYY-MM-DD")
    parser.add_argument("--outfile", type=str, help="Path to the json file where the output will be stored")

    args = parser.parse_args()
    return args


def main():
    args = get_args_from_command_line()
    consumer_key, consumer_secret, bearer_token = retrieve_keys(keys_folder_name=args.keys_folder_name)
    client = tweepy.Client(consumer_key=consumer_key,
                           consumer_secret=consumer_secret,
                           bearer_token=bearer_token,
                           wait_on_rate_limit=True)
    query = build_search_query_keywords(keywords_path=args.keywords_path, lang=args.lang)
    logger.info(f'Search query: {query}')
    if args.count_method == 'full_archive':
        start_time = f'{args.start_time}T00:00:00.000Z'
        response = client.get_all_tweets_count(query=query, start_time=start_time, granularity='day')
        nb_tweets = response.meta['total_tweet_count']
        save_to_json(data_dict=response.data, outfile=args.outfile)
        while 'next_token' in response.meta.keys():
            logger.info(f'Going back to {response.data[0]["end"]}')
            response = client.get_all_tweets_count(query=query, granularity='day',
                                                   start_time=start_time,
                                                   next_token=response.meta['next_token'])
            nb_tweets = nb_tweets + response.meta['total_tweet_count']
            save_to_json(data_dict=response.data, outfile=args.outfile)
    elif args.count_method == 'recent':
        response = tweepy.Paginator(client.get_recent_tweets_count, query=query, granularity='day')
        nb_tweets = response.meta['total_tweet_count']
    logger.info(f'# tweets: {nb_tweets}')

if __name__ == '__main__':
    main()
