import logging
import psycopg2
from psycopg2 import sql
import requests
import re
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from config import DB_CONFIG, OLD_TABLE_CONFIG, NEW_TABLE_CONFIG, MODERATE_HATE_SPEECH_API_TOKEN

# Download VADER lexicon
nltk.download('vader_lexicon')

# Configure logging
logging.basicConfig(filename='script.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

c = 0

def hs_check_comment(comment):
    CONF_THRESHOLD = 0.9

    data = {
        "token": MODERATE_HATE_SPEECH_API_TOKEN,
        "text": comment
    }

    try:
        response = requests.post("https://api.moderatehatespeech.com/api/v1/moderate/", json=data)
        response.raise_for_status()  # Check for HTTP errors

        # Skip empty responses
        if not response.text:
            logging.warning("Empty JSON response, skipping.")
            return False

        try:
            response_json = response.json()

            # Check if the expected keys are present in the response
            class_value = response_json.get("class")
            confidence_value = response_json.get("confidence")

            if class_value == "flag" and confidence_value is not None and float(confidence_value) > CONF_THRESHOLD:
                logging.info("Hate speech detected: %s", comment)
                return True
            return False

        except requests.exceptions.JSONDecodeError:
            logging.error(f"Failed to decode JSON response: {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")

    return False

def clean_comment(comment):
    # Remove both http and https links from the comment
    comment_without_links = re.sub(r'https?://\S+', '', comment)

    # Implement your additional comment cleaning logic here
    # Example: Remove special characters and convert to lowercase
    cleaned_comment = re.sub(r'[^a-zA-Z0-9\s]', '', comment_without_links).lower()

    return cleaned_comment

def create_table(cursor, table_name, columns):
    # Construct column definitions with data types
    column_definitions = [
        sql.Identifier(column_name) + sql.SQL(' ') + sql.SQL(data_type)
        for column_name, data_type in columns.items()
    ]

    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            {}
        )
    """).format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(column_definitions)
    )

    print(create_table_query.as_string(cursor))
    cursor.execute(create_table_query)


def is_comment_id_present(cursor, table_name, comment_id):
    select_query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE comment_id = %s)").format(
        sql.Identifier(table_name)
    )
    cursor.execute(select_query, (comment_id,))
    return cursor.fetchone()[0]

def process_comments():
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    # Create the new table if not exists
    create_table(cursor, NEW_TABLE_CONFIG["name"], NEW_TABLE_CONFIG["columns"])

    # Select comments from the old table
    select_query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(', ').join(map(sql.Identifier, OLD_TABLE_CONFIG["columns"])),
        sql.Identifier(OLD_TABLE_CONFIG["name"])
    )
    cursor.execute(select_query)
    comments = cursor.fetchall()

    # Clean, check for hate speech, and update comments into the new table
    for comment_data in comments:
        # Extract relevant fields based on the configuration
        comment_id = comment_data[0]
        video_id = comment_data[1]
        comment_text = comment_data[2]

        if is_comment_id_present(cursor, NEW_TABLE_CONFIG["name"], comment_id):
            logging.info("Comment ID %s already present, skipping.", comment_id)
            continue

        # Implement your additional comment cleaning logic here
        cleaned_comment = clean_comment(comment_text)

        is_hate_speech = hs_check_comment(cleaned_comment)

        # Analyze sentiment
        sentiment = analyze_sentiment(cleaned_comment)

        # Insert into the new table
        insert_query = sql.SQL("""
            INSERT INTO {} ("comment_id", "video_id", "original_comment", "cleaned_comment", "is_hate_speech", "sentiment")
            VALUES (%s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(NEW_TABLE_CONFIG["name"]))

        values = (comment_id, video_id, comment_text, cleaned_comment, is_hate_speech, sentiment)
        cursor.execute(insert_query, values)

        # Insert records into the new table after every count of 1000
        if c % 1000 == 0:
            connection.commit()

    connection.commit()
    connection.close()

def analyze_sentiment(comment):
    # Check if the comment is a string
    if isinstance(comment, str):
        analyzer = SentimentIntensityAnalyzer()
        sentiment_scores = analyzer.polarity_scores(comment)

        # Classify the sentiment
        if sentiment_scores['compound'] >= 0.05:
            logging.info("Positive sentiment detected: %s", comment)
            return 'positive'
        elif sentiment_scores['compound'] <= -0.05:
            logging.info("Negative sentiment detected: %s", comment)
            return 'negative'
        else:
            logging.info("Neutral sentiment detected: %s", comment)
            return 'neutral'
    else:
        # Handle non-string values (e.g., float)
        return 'not a string'

if __name__ == "__main__":
    process_comments()
