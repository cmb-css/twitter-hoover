import json
from collections import Counter


class YouTubeVideos():
    def __init__(self):
        self.videos = Counter()

    def find_videos(self, json_data):
        if 'extended_tweet' in json_data:
            if 'entities' in json_data['extended_tweet']:
                if 'urls' in json_data['extended_tweet']['entities']:
                    for url in json_data['extended_tweet']['entities']['urls']:
                        if 'expanded_url' in url:
                            url_str = url['expanded_url']
                            if 'youtube.com/watch?v=' in url_str:
                                url_str = url_str.replace('http://',
                                                          'https://')
                                url_str = url_str.replace('https://m.',
                                                          'https://www.')
                                url_str = url_str.replace('https://y',
                                                          'https://www.y')
                                url_str = url_str[:43]
                                self.videos[url_str] += 1

    def process(self, infile):
        with open(infile, 'r') as file:
            for line in file:
                data = json.loads(line)
                self.find_videos(data)
                if 'retweeted_status' in data:
                    self.find_videos(data['retweeted_status'])
                if 'quoted_status' in data:
                    self.find_videos(data['quoted_status'])

    def output_csv(self):
        for url, count in self.videos.most_common():
            print('{}, {}'.format(url, count))


def extract_videos(infile):
    yt = YouTubeVideos()
    yt.process(infile)
    yt.output_csv()
