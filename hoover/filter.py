def clean_keyword(keyword):
    if len(keyword) > 0 and keyword[0] == '#':
        return keyword[1:]
    else:
        return keyword


def read_keywords(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    keywords = data.split('\n')
    keywords = [clean_keyword(keyword.strip()) for keyword in keywords
                if len(keyword.strip()) > 0]
    return keywords


def create_filter(file_path):
    keywords = read_keywords(file_path)
    return ','.join(keywords)
