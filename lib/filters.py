# Module dedicated to the implementation of the filters used in the queries

# Reviews dataset format:
# id, title, price, user_id, profile_name, helpfulness, score, time, summary, text
# 0 , 1    , 2    , 3      , 4           , 5          , 6    , 7   , 8      , 9

# Books dataset format:
# title, description, authors, image, preview_link, publisher, published_date, info_link, categories, ratings_count
# 0    , 1          , 2      , 3    , 4           , 5        , 6             , 7        , 8         , 9

import re
from textblob import TextBlob

COUNTER_FIELD = 'counter'
TITLE_FIELD = 'Title'


# Generic filter that returns the desired rows from a dataset according to a given condition and value
def filter_by(batch, condition, values):
    """
    Filter the batch by a given condition function and values
    """
    return [row_dictionary for row_dictionary in batch if condition(row_dictionary, values)]

def title_condition(row_dictionary, value):
    """
    Check if the title of the item is in the values list
    """
    return value.lower() in row_dictionary['Title'].lower()

def category_condition(row_dictionary, value):
    """
    Check if the category of the item is in the values list
    """
    title_category = row_dictionary['categories']
    title_category = re.sub(r'[^a-zA-Z]', '', title_category)

    return value == title_category 

def review_quantity_value(batch, value):
    """
    Check if the the review quantity is greater than the value
    If it is, then we add it to the result list of titles information
    """
    filtered_dict = {}
    for row_dictionary in batch:
        for title, values in row_dictionary.items():
            if int(values.split(',')[0]) >= value:
                filtered_dict[title] = values
            
    return [filtered_dict]

def year_range_condition(row_dictionary, values):
    """
    Check if the published date of the item is in the values list
    """
    year_regex = re.compile('(?<!\*)[^\d]*(\d{4})[^\d]*(?<!\*)')
    result = year_regex.search(row_dictionary['publishedDate'])
    try:
        year = int(result.group(1))
    except:
        return False
 
    return values[0] <= year <= values[1]

def different_decade_counter(batch):
    """
    Summarize the number of different decades in which
    each author published a book
    """
    authors_dict = {}
    # Get the decades for each author
    for row_dict in batch:
        if not row_dict['publishedDate'] or not row_dict['authors']:
            continue
        authors = row_dict['authors']
        splitted_authors = re.sub(r'[^\w,\s]', '',authors).split(',')
        year = row_dict['publishedDate'].split('-')[0]
        year = re.sub(r'\D', '', year)
        year = int(year)
        for author in splitted_authors:
            if author == '':
                continue
            elif author.startswith(' ') or author.endswith(' '):
                author = author.lstrip(' ').rstrip(' ')
            
            if author not in authors_dict:
                authors_dict[author] = set()

            authors_dict[author].add(str(year - year%10))

    return [authors_dict]

def parse_authors_dict(authors_dict):
    authors_decades = {}
    for author, decades_str in authors_dict.items():
        splitted_val = decades_str.split(',')
        decades = set()
        for decade in splitted_val:
            decades.add(decade)
        authors_decades[author] = decades

    return authors_decades

def accumulate_authors_decades(batch, decades_accumulator):
    authors_decades = parse_authors_dict(batch[0])

    for author, decades_set in authors_decades.items():
        if author not in decades_accumulator:
            decades_accumulator[author] = set()
        decades_accumulator[author] = decades_accumulator[author].union(decades_set)


def calculate_review_sentiment(batch):
    """
    Calculate the sentiment of the reviews
    """
    result = []
    for row_dictionary in batch:
        sentiment_dict = {}
        text = row_dictionary['review/text']
        if text == '':
            continue
        title = row_dictionary['Title']
        blob = TextBlob(text)
        text_sentiment = blob.sentiment.polarity
        sentiment_dict['Title'] = title
        sentiment_dict['text_sentiment'] = str(text_sentiment)
        result.append(sentiment_dict)
    return result

def calculate_percentile(sentiment_scores, percentile):
    """
    Calculate the titles above a certain percentile
    """
    titles = []
    for title, score in sentiment_scores.items():
        if score > percentile:
            titles.append(title) # TODO: This is much more complex than this
    return titles


def get_top_n(batch, top, top_n, last):
    batch_top = []
    for title_dict in batch:
        title = title_dict[TITLE_FIELD]
        counter = title_dict[COUNTER_FIELD]
        if last:
            mean_rating = counter
        else:
            splitted_counter = counter.split(',', 2) # Only split until the second ','. This is to not split the authors field which may have also a ','.
            mean_rating = float(splitted_counter[1]) / float(splitted_counter[0])

        batch_top.append({TITLE_FIELD: title, COUNTER_FIELD: mean_rating})

    batch_top.sort(key=sorting_key, reverse=True)
    top = top + batch_top[:top_n]
    top.sort(key=sorting_key, reverse=True)
    return top[:top_n]


def sorting_key(title_dict):
    return title_dict[COUNTER_FIELD]

def titles_in_the_n_percentile(review_sentiment_dict, n):
    vals = list(review_sentiment_dict.values())
    vals = sorted(vals)
    percentile_index = int(len(vals) * 0.9)
    percentile_val = vals[percentile_index]
    percentile_val = 0.37467883042883043
    titles_in_percentile_n = [title for title, valor in review_sentiment_dict.items() if valor >= percentile_val]
    return titles_in_percentile_n

def handle_eof(method, body, eof_counter, worker_quantity, data_output_name, next_worker_quantity, middleware):
    if body != b'EOF':
        print("[ERROR] Not an EOF on handle_eof(), system BLOCKED!. Received: ", body)
    
    eof_counter[0] += 1
    if eof_counter[0] == worker_quantity:
        for _ in range(next_worker_quantity):
            middleware.send_message(data_output_name, 'EOF')
        middleware.stop_consuming()

    middleware.ack_message(method)

def eof_manage_process(worker_id, worker_quantity, next_worker_quantity, data_output_name, middleware, eof_queue):
    if worker_id == '0':
        if worker_quantity == 1:
            for _ in range(next_worker_quantity):
                middleware.send_message(data_output_name, 'EOF')
            return
        eof_counter = [0]
        eof_callback = lambda ch, method, properties, body: handle_eof(method, body, eof_counter, worker_quantity - 1, data_output_name, next_worker_quantity, middleware)
        middleware.receive_messages(eof_queue, eof_callback)
        middleware.consume()
    else:
        middleware.send_message(eof_queue, 'EOF')




