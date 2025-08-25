import schedule
import time
import requests
import csv
import json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from html import unescape
import psycopg2
import pandas as pd
conn = psycopg2.connect(
    dbname="aassemble",
    user="hkatakam",
    password="BPyjmXf99",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
cur.execute('''
    CREATE TABLE IF NOT EXISTS Thread (
        id SERIAL PRIMARY KEY,
        PostNumber TEXT,
        Comment TEXT,
        Timestamp TEXT,
        Name TEXT,
        ImageFilename TEXT
    )
''')
cur.execute('''
    CREATE TABLE IF NOT EXISTS Catalog (
        id SERIAL PRIMARY KEY,
        PageNumber TEXT,
        PostNumber TEXT,
        Timestamp TEXT,
        Name TEXT,
        Comment TEXT,
        ImageFilename TEXT
    )
''')
def get_threads(board, page):
    base_url = f"https://a.4cdn.org/{board}/{page}.json"
    response = requests.get(base_url)
    print(base_url)
    if response.status_code == 200:
        return response.text
    else:
        return None
def get_catalog(board):
    base_url = f"https://a.4cdn.org/{board}/catalog.json"
    response = requests.get(base_url)
    print(base_url)
    if response.status_code == 200:
        return response.text
    else:
        return None
def clean_comment(text):
    soup = BeautifulSoup(text, 'html.parser')
    cleaned_text = soup.get_text()
    cleaned_text = unescape(cleaned_text)
    cleaned_text = cleaned_text.replace(">>", "")
    cleaned_text = cleaned_text.replace(">", "")
    return cleaned_text

def crawl_4chan(board, limit=10):
    posts = []
    catalogs = []
    page = 1
    current_time = datetime.now()
    while len(posts) < limit:
        threads = get_threads(board, page)
        if not threads:
            break
        json_data = json.loads(threads)
        for thread in json_data['threads']:
            for post in thread['posts']:
                timestamp = datetime.fromtimestamp(post.get('time', 0))
                if current_time - timestamp < timedelta(minutes=3):
                    post_info = {
                        'Post Number': post.get('no', ''),
                        'Comment': clean_comment(post.get('com', '')),
                        'Timestamp': post.get('now', ''),
                        'Name': post.get('name', ''),
                        'Image Filename': post.get('filename', ''),
                    }
                    posts.append(post_info)
        page += 1
    catalogs_data = get_catalog(board)
    if catalogs_data:
        catalog_json = json.loads(catalogs_data)
        for page in catalog_json:
            for thread in page['threads']:
                timestamp = datetime.fromtimestamp(thread.get('time', 0))
                if current_time - timestamp < timedelta(minutes=3):
                    catalog_info = {
                        'Page Number': page['page'],
                        'Post Number': thread.get('no', ''),
                        'Timestamp': thread.get('now', ''),
                        'Name': thread.get('name', ''),
                        'Comment': clean_comment(thread.get('com', '')),
                        'Image Filename': thread.get('filename', ''),
                    }
                    catalogs.append(catalog_info)

    for post in posts[:limit]:
        cur.execute(
            "INSERT INTO Thread (PostNumber, Comment, Timestamp, Name, ImageFilename) VALUES (%s, %s, %s, %s, %s)",
            (post['Post Number'], post['Comment'], post['Timestamp'], post['Name'], post['Image Filename'])
        )

    for catalog in catalogs:
        cur.execute(
            "INSERT INTO Catalog (PageNumber, PostNumber, Timestamp, Name, Comment, ImageFilename) VALUES (%s, %s, %s, %s, %s, %s)",
            (catalog['Page Number'], catalog['Post Number'], catalog['Timestamp'], catalog['Name'], catalog['Comment'], catalog['Image Filename'])
        )
    conn.commit()
def main():
    r = pd.read_csv("boards.csv")
    for redd in r['title']:
        print(redd)
        board_name = redd  
        number_of_posts = 10  
        crawl_4chan(board_name, number_of_posts)
if __name__ == "__main__":
    main()
    schedule.every(3).minutes.do(main)
    while True:
        schedule.run_pending()
        time.sleep(1)
