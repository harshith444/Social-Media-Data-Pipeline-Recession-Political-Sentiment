import requests
from datetime import datetime, timedelta
import pandas as pd
import csv
import psycopg2
import schedule
import time
from Data_config import API_KEYS, DB_CONFIG
CACHE = {}
current_key_index = 0

def search_videos_by_keyword(keyword):
    if keyword in CACHE:
        return CACHE[keyword]
    url = f"https://www.googleapis.com/youtube/v3/search"
    params = {
        "q": keyword,
        "type": "video",
        "part": "id,snippet",
        "maxResults": 200,
        "key": API_KEYS[current_key_index],
        "alt": "json"
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        video_data = [{'VideoID': item['id']['videoId'], 'VideoTitle': item['snippet']['title']} for item in data.get('items', [])]
        CACHE[keyword] = video_data
        return video_data
    else:
        print(f"Request failed with status code {response.status_code}")
        return []
def switch_api_key():
    global current_key_index
    current_key_index = (current_key_index + 1) % len(API_KEYS)
    print(API_KEYS[current_key_index])

def get_current_api_key():
    print(API_KEYS[current_key_index])
    return API_KEYS[current_key_index]

def get_comments(video_data):
    comments = []
    now = datetime.utcnow()
    fetch_time = now - timedelta(hours=12)

    for video_info in video_data:
        video_id = video_info['VideoID']
        video_title = video_info['VideoTitle']
        try:
            if video_id in CACHE:
                video_comments = CACHE[video_id]
            else:
                video_comments = fetch_comments_for_video(video_id, video_title)
                CACHE[video_id] = video_comments

            for comment in video_comments:
                comment_time = datetime.fromisoformat(comment['CommentTime'][:-1]) 
                if fetch_time <= comment_time <= now:
                    comments.append(comment)
        except Exception as e:
            print(f"Error occurred while processing video {video_id}: {e}")

    return comments

def fetch_comments_for_video(video_id, video_title):
    url = f"https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "textFormat": "plainText",
        "maxResults": 100,
        "key": API_KEYS[current_key_index]
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        video_comments = [{'VideoID': video_id, 'VideoTitle': video_title, 'CommentID': item['id'],
                           'CommentTime': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                           'CommentText': item['snippet']['topLevelComment']['snippet']['textDisplay']}
                          for item in data.get('items', [])]
        return video_comments
    else:
        print(f"Request failed with status code {response.status_code}")
        return []
def create_table_if_not_exists():
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()
    create_table_query = '''CREATE TABLE IF NOT EXISTS yt_comments
          (ID SERIAL PRIMARY KEY NOT NULL,
          VIDEO_ID TEXT NOT NULL,
          VIDEO_TITLE TEXT NOT NULL,
          COMMENT_ID TEXT NOT NULL,
          COMMENT_TIME TEXT NOT NULL,
          COMMENT_TEXT TEXT NOT NULL); '''

    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()

def insert_comments_to_postgres(comments):
    connection = psycopg2.connect(**DB_CONFIG)

    cursor = connection.cursor()

    for comment in comments:
        check_query = "SELECT COUNT(*) FROM yt_comments WHERE VIDEO_ID = %s AND COMMENT_ID = %s"
        check_params = (comment['VideoID'], comment['CommentID'])
        cursor.execute(check_query, check_params)
        existing_comments_count = cursor.fetchone()[0]

        if existing_comments_count == 0:
            insert_query = """INSERT INTO yt_comments (VIDEO_ID, VIDEO_TITLE, COMMENT_ID, COMMENT_TIME, COMMENT_TEXT) 
                              VALUES (%s, %s, %s, %s, %s)"""
            record_to_insert = (comment['VideoID'], comment['VideoTitle'], comment['CommentID'],
                                comment['CommentTime'], comment['CommentText'])
            cursor.execute(insert_query, record_to_insert)

    connection.commit()
    cursor.close()
    connection.close()
def job():
    print("Job is running at", datetime.now())
    create_table_if_not_exists()
    r = pd.read_csv("/home/hkatakam/dbtest/Youtube_key.csv")
    for i in r['title']:
        print(i)
        keyword = i  
        video_data = search_videos_by_keyword(keyword)
        comments = get_comments(video_data)
        insert_comments_to_postgres(comments)
        switch_api_key()
    CACHE.clear()
    print("Job is running at", datetime.now())

if __name__ == '__main__':
    now1 =datetime.now()
    print(now1)
    schedule.every(60).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(30)
