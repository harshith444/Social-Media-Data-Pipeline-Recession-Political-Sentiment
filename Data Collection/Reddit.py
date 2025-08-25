
import requests
import pandas as pd
import psycopg2
import time
import datetime
import logging
import schedule
from Data_config import REDDIT_API_CONFIG, DB_CONFIG, TARGET_TABLE

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
client_id = REDDIT_API_CONFIG['client_id']
client_secret = REDDIT_API_CONFIG['client_secret']
username = REDDIT_API_CONFIG['username']
password = REDDIT_API_CONFIG['password']
db_config = DB_CONFIG
dbname = db_config['dbname']
user = db_config['user']
password_db = db_config['password']
host = db_config['host']
subreddits_df = pd.read_csv('/home/hkatakam/dbtest/subreddits.csv')
subreddits = subreddits_df['subreddit'].tolist()
MAX_REQUESTS_PER_MINUTE = 100
RATE_LIMIT_RESET_SECONDS = 60
headers = {'User-Agent': 'myproj/0.0.1'}

def fetch_comments_for_subreddit(subreddit):
    global data
    logging.info(f"Fetching comments for subreddit: {subreddit}")
    data = pd.DataFrame()
    after = None
    current_time = time.time()
    requests_made = 0
    last_request_time = time.time()
    for i in range(1):
        logging.info(f"Fetching comments for day ")
        if requests_made >= MAX_REQUESTS_PER_MINUTE:
            elapsed_time = time.time() - last_request_time
            if elapsed_time < RATE_LIMIT_RESET_SECONDS:
                sleep_time = RATE_LIMIT_RESET_SECONDS - elapsed_time
                logging.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
            requests_made = 0
            last_request_time = time.time()
        
        start_of_day = datetime.datetime.utcfromtimestamp(current_time - (i + 1) * 86400).strftime('%Y-%m-%d')
        end_of_day = datetime.datetime.utcfromtimestamp(current_time - i * 86400).strftime('%Y-%m-%d')      
        url = f"https://oauth.reddit.com/r/{subreddit}/comments?sort=new&time_filter=all&before={end_of_day}&after={start_of_day}"
        logging.debug(f"Fetching comments from {url}")
        res = requests.get(url, headers=headers)
        rate_limit_used = int(float(res.headers.get('X-Ratelimit-Used', 0)))
        rate_limit_remaining = int(float(res.headers.get('X-Ratelimit-Remaining', 0)))
        rate_limit_reset = int(float(res.headers.get('X-Ratelimit-Reset', 0)))
        requests_made += rate_limit_used
        last_request_time = time.time()
        if rate_limit_remaining <= 0:
            sleep_time = max(1, rate_limit_reset)  
            logging.info(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
        try:
            comment_data = df_from_response(res)
        except Exception as e:
            logging.error(f"Error processing response: {e}")
            continue
        if comment_data.empty:
            logging.info(f"No comments to insert into PostgreSQL for subreddit: {subreddit}")
            break
        comment_df = comment_data
        data = pd.concat([data, comment_df], ignore_index=True)
        row = comment_df.iloc[-1]
        after = row['id']
    comment_df_dict = data.to_dict("records")
    if comment_df_dict:
        for comment in comment_df_dict:
            check_query = f"SELECT EXISTS(SELECT 1 FROM {TARGET_TABLE} WHERE comment_id = %s)"
            cursor.execute(check_query, (comment['id'],))
            comment_exists = cursor.fetchone()[0]
            if comment_exists:
                logging.info(f"Comment with ID {comment['id']} already exists in the database. Skipping.")
                continue  
            created_utc = datetime.datetime.utcfromtimestamp(comment['created_utc'])
            insert_query = f"""
            INSERT INTO {TARGET_TABLE} (subreddit, post_id, body, score, created_utc, comment_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                comment['subreddit'],
                comment['post_id'],
                comment['body'],
                comment['score'],
                created_utc,  
                comment['id']
            ))
            rows_inserted = cursor.rowcount
            logging.info(f"Inserted {rows_inserted} row(s) for comment with ID {comment['id']}.")
        connection.commit()
        logging.info(f"Inserted {len(comment_df_dict)} comments into the database for subreddit {subreddit}.")
    else:
        logging.info(f"No comments to insert into PostgreSQL for subreddit: {subreddit}")

def df_from_response(res):
    comment_data = []
    try:
        json_data = res.json()
    except ValueError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        return pd.DataFrame()
    if 'data' in json_data:
        for comment in json_data['data'].get('children', []):
            try:
                comment_dict = {
                    'subreddit': comment['data']['subreddit'],
                    'post_id': comment['data']['link_id'],
                    'body': comment['data']['body'],
                    'score': comment['data'].get('score', 0),
                    'created_utc': comment['data']['created_utc'],
                    'id': comment['data']['id']
                }
                comment_data.append(comment_dict)
            except KeyError as e:
                logging.error(f"KeyError while extracting comment data: {e}")
    comment_df = pd.DataFrame(comment_data)
    return comment_df

# Authentication with Reddit API
client_auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
data = {
    'grant_type': 'password',
    'username': username,
    'password': password
}
headers = {'User-Agent': 'myproj/0.0.1'}
response = requests.post('https://www.reddit.com/api/v1/access_token', auth=client_auth, data=data, headers=headers)

if response.status_code == 200:
    access_token = response.json()['access_token']
    headers['Authorization'] = f'Bearer {access_token}'
    logging.info("Successfully authenticated with Reddit API.")
else:
    logging.error(f"Authentication failed. Status code: {response.status_code}")

# Connection to PostgreSQL
connection = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password_db,
    host=host,
)
cursor = connection.cursor()

create_table_query = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
    id serial PRIMARY KEY,
    subreddit text,
    post_id text,
    body text,
    score integer,
    created_utc timestamp,
    comment_id text
);
"""
cursor.execute(create_table_query)
connection.commit()

# Schedule data fetching for each subreddit
for subreddit in subreddits:
    schedule.every(1).seconds.do(fetch_comments_for_subreddit, subreddit=subreddit)
    logging.info(f"Scheduled data fetching for subreddit: {subreddit}")

# Run the schedule
while True:
    schedule.run_pending()
    time.sleep(1)
