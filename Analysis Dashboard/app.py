import sys
import datetime
import io
import base64
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.dates import DateFormatter
import mpld3
import numpy as np
import configparser
from flask import Flask, render_template, Response,flash, request, redirect, url_for
import os

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key')
config = configparser.ConfigParser()
config.read('config.ini')
database_config = {
    'database': config['Database']['database'],
    'user': config['Database']['user'],
    'password': config['Database']['password'],
    'host': config['Database']['host'],
}

db_conn = psycopg2.connect(**database_config)
def plot_politics_comments(start_date=None, end_date=None):
    x_axis = []
    y_axis = []
    count = 0
    start = None    
    query = """
        SELECT created_utc FROM politics
        WHERE created_utc >= %s AND created_utc < %s
        ORDER BY created_utc;
    """
    df = pd.read_sql_query(query, db_conn, params=(start_date, end_date))
    if(df.empty):
        flash("Data not available for the selected date range", 'politics_comments')
        return redirect(url_for('index'))
    else:       
        for index, row in df.iterrows():
            timestamp = row['created_utc']
            if start is None:
                start = timestamp
                print(timestamp)
            count += 1
            endtime = timestamp
            if int(start.date().strftime("%d")) != int(endtime.date().strftime("%d")):
                x_axis.append(start)
                y_axis.append(count)
                start = None
                count = 0

        if start not in x_axis:
            x_axis.extend([start, start + datetime.timedelta(days=1)])
            y_axis.extend([count, count / 10 * 5.5])

        plt.rcParams["figure.figsize"] = [12, 6]
        fig, ax = plt.subplots()
        ax.plot(x_axis, y_axis, "-o")
        ax.xaxis.set_major_locator(plt.MaxNLocator(15))
        ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))

        
        return mpld3.fig_to_html(fig)
def plot_data_counts(start_date=None, end_date=None):
    reddit_query = "SELECT COUNT(*) as count FROM comments_for_reddits WHERE created_utc >= %s AND created_utc < %s;"
    chan_query = "SELECT COUNT(*) as count FROM thread WHERE DATE(timestamp) >= %s AND DATE(timestamp) < %s;"
    youtube_query = "SELECT COUNT(*) as count FROM yt_comments WHERE comment_time >= %s AND comment_time < %s;"

    # Fetch data into DataFrames
    reddit_count = pd.read_sql_query(reddit_query, db_conn, params=(start_date, end_date))['count'].values
    chan_count = pd.read_sql_query(chan_query, db_conn, params=(start_date, end_date))['count'].values
    youtube_count = pd.read_sql_query(youtube_query, db_conn, params=(start_date, end_date))['count'].values
    # print("Reddit Count:", reddit_count)
    # print("4chan Count:", chan_count)
    # print("YouTube Count:", youtube_count)
    if (reddit_count.size == 0 and chan_count.size == 0 and youtube_count.size == 0):
        flash("Data not available for the selected date range", 'data_counts')
        return redirect(url_for('index'))
    else:
        labels = ['Reddit', '4chan', 'YouTube']
        counts = [
            reddit_count[0] if reddit_count.size else 0,
            chan_count[0] if chan_count.size else 0,
        youtube_count[0] if youtube_count.size else 0
        ]
        plt.figure(figsize=(10, 6))
        bars = plt.bar(labels, counts, color=['blue', 'green', 'red'])
        plt.title('Data Count from Different APIs')
        plt.xlabel('APIs')
        plt.ylabel('Count')
        for bar, label in zip(bars, labels):
            plt.text(
                bar.get_x() + bar.get_width() / 2, bar.get_height(), label, ha='center', va='bottom', fontsize=16
            )
        return mpld3.fig_to_html(plt.gcf())


#Sentiment Analysis
def chan4_query_database(start_date, end_date):
    chan4_query_sentiment = f"SELECT s.sentiment, COUNT(*) as count FROM an_4chan s JOIN thread p ON s.postnumber=p.postnumber WHERE TO_CHAR(p.timestamp::date, 'YYYY-MM-DD') BETWEEN '{start_date}' AND '{end_date}' GROUP BY sentiment;"
    chan4_quer = pd.read_sql(chan4_query_sentiment, db_conn)
    return chan4_quer
def reddit_query_database(start_date, end_date):
    query = f"SELECT s.sentiment, COUNT(*) as count FROM an_r_all_score s JOIN comments_for_reddits p ON s.comment_id=p.comment_id WHERE p.created_utc BETWEEN '{start_date}' AND '{end_date}' GROUP BY sentiment;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_sentiment_analysis_reddit(sentiment_df):
    if(sentiment_df.empty):
        flash("Data not available for the selected date range", 'sentiment_reddit')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        sentiment_df.plot(kind='bar', x='sentiment', y='count', color='green', fontsize=6, width=0.3)
        plt.title('Sentiment Analysis for Reddit', fontsize=20)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Sentiment', fontsize=12)
        #saving plot
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)

        plot_data_reddit = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_data_reddit

def plot_sentiment_analysis_4chan(sentiment_df):
    if(sentiment_df.empty):
        flash("Data not available for the selected date range", 'sentiment_4chan')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        sentiment_df.plot(kind='bar', x='sentiment', y='count', color='green', fontsize=6, width=0.3)
        plt.title('Sentiment Analysis for 4chan', fontsize=30)
        plt.ylabel('Count', fontsize=16)
        plt.xlabel('Sentiment', fontsize=16)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_data_4chan = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_data_4chan



def youtube_query_database(start_date, end_date):
    query = f"SELECT s.sentiment, COUNT(*) as count FROM an_yt1 s JOIN yt_comments p ON s.comment_id=p.comment_id WHERE p.comment_time BETWEEN '{start_date}' AND '{end_date}' GROUP BY sentiment;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_sentiment_analysis_youtube(sentiment_df):
    if(sentiment_df.empty):
        flash("Data not available for the selected date range", 'sentiment_youtube')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        sentiment_df.plot(kind='bar', x='sentiment', y='count', color='green', fontsize=6, width=0.3)
        plt.title('Sentiment Analysis for YouTube', fontsize=20)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Sentiment', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_data_youtube = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return plot_data_youtube

def politics_query_database(start_date, end_date):
    query = f"SELECT s.sentiment, COUNT(*) as count FROM an_r_poli_score  s JOIN politics p ON s.comment_id=p.comment_id WHERE p.created_utc BETWEEN '{start_date}' AND '{end_date}' GROUP BY sentiment;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_sentiment_analysis_politics(sentiment_df):
    if(sentiment_df.empty):
        flash("Data not available for the selected date range", 'sentiment_politics')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        sentiment_df.plot(kind='bar', x='sentiment', y='count', color='green', fontsize=6, width=0.3)
        plt.title('Sentiment Analysis for YouTube', fontsize=20)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Sentiment', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_data_politics = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_data_politics

#HateSpeech Analysis
def hatespeech_reddit_query_database(start_date, end_date):
    query = f"SELECT s.is_hate_speech, COUNT(*) as count FROM an_r_all_score s JOIN comments_for_reddits p ON s.comment_id=p.comment_id WHERE p.created_utc BETWEEN '{start_date}' AND '{end_date}' GROUP BY is_hate_speech;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_hatespeech_analysis_reddit(hate_speech_reddit_df):
    if(hate_speech_reddit_df.empty):
        flash("Data not available for the selected date range", 'hatespeech_reddit')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        hate_speech_reddit_df.plot(kind='bar', x='is_hate_speech', y='count', color='red', fontsize=8, width=0.3)
        plt.title('Hate Speech Analysis for Reddit', fontsize=16)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Hate Speech', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_hatespeech_data_reddit = base64.b64encode(img.getvalue()).decode()
        plt.close()

        return plot_hatespeech_data_reddit


def hatespeech_4chan_query_database(start_date, end_date):
    query = f"SELECT s.is_hate_speech, COUNT(*) as count FROM an_4chan_score s JOIN thread p ON s.comment_id=p.postnumber WHERE TO_CHAR(p.timestamp::date, 'YYYY-MM-DD') BETWEEN '{start_date}' AND '{end_date}' GROUP BY is_hate_speech;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_hatespeech_analysis_4chan(hate_speech_4chan_df):
    if(hate_speech_4chan_df.empty):
        flash("Data not available for the selected date range", 'hatespeech_4chan')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        hate_speech_4chan_df.plot(kind='bar', x='is_hate_speech', y='count', color='red', fontsize=8, width=0.3)
        plt.title('Hate Speech Analysis for 4chan', fontsize=16)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Hate Speech', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_hatespeech_data_4chan = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_hatespeech_data_4chan

def hatespeech_youtube_query_database(start_date, end_date):
    query = f"SELECT s.is_hate_speech, COUNT(*) as count FROM an_yt1 s JOIN yt_comments p ON s.comment_id=p.comment_id WHERE p.comment_time BETWEEN '{start_date}' AND '{end_date}' GROUP BY is_hate_speech;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_hatespeech_analysis_youtube(hate_speech_youtube_df):
    if(hate_speech_youtube_df.empty):
        flash("Data not available for the selected date range", 'hatespeech_youtube')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        hate_speech_youtube_df.plot(kind='bar', x='is_hate_speech', y='count', color='red', fontsize=8, width=0.3)
        plt.title('Hate Speech Analysis for 4chan', fontsize=16)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Hate Speech', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_hatespeech_data_youtube = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_hatespeech_data_youtube
def hatespeech_politics_query_database(start_date, end_date):
    query = f"SELECT s.is_hate_speech, COUNT(*) as count FROM an_r_poli_score  s JOIN politics p ON s.comment_id=p.comment_id WHERE p.created_utc BETWEEN '{start_date}' AND '{end_date}' GROUP BY is_hate_speech;"
    result = pd.read_sql(query, db_conn)
    return result

def plot_hatespeech_analysis_politics(hate_speech_politics_df):
    if(hate_speech_politics_df.empty):
        flash("Data not available for the selected date range", 'hatespeech_politics')
        return redirect(url_for('index'))
    else:
        plt.figure(figsize=(10, 6))
        hate_speech_politics_df.plot(kind='bar', x='is_hate_speech', y='count', color='red', fontsize=8, width=0.3)
        plt.title('Hate Speech Analysis for politics', fontsize=16)
        plt.ylabel('Count', fontsize=12)
        plt.xlabel('Hate Speech', fontsize=12)
        img = io.BytesIO()
        plt.savefig(img, format='png')
        img.seek(0)
        plot_hatespeech_data_politics = base64.b64encode(img.getvalue()).decode()
        plt.close()
        return plot_hatespeech_data_politics
@app.route('/')
def index():
    return render_template('index.html')
@app.route('/plot_politics_comments')
def plot_politics_comments_route():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    plot_html = plot_politics_comments(start_date, end_date)
    return render_template('index.html', politics_comments_plot=plot_html, error_message=None)

@app.route('/plot_data_counts')
def plot_data_counts_route():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    plot_html = plot_data_counts(start_date, end_date)
    return render_template('index.html', data_counts_plot=plot_html)

@app.route('/hatespeech_analysis_reddit', methods=['GET', 'POST'])
def hatespeech_reddit_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        hatespeech_df = hatespeech_reddit_query_database(start_date, end_date)
        plot_hatespeech_data_reddit = plot_hatespeech_analysis_reddit(hatespeech_df)
        return render_template('index.html', plot_hatespeech_data_reddit=plot_hatespeech_data_reddit)
    return render_template('index.html', plot_hatespeech_data_reddit=None)

@app.route('/hatespeech_analysis_4chan', methods=['GET', 'POST'])
def hatespeech_4chan_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        hatespeech_df = hatespeech_4chan_query_database(start_date, end_date)
        plot_hatespeech_data_4chan = plot_hatespeech_analysis_4chan(hatespeech_df)
        return render_template('index.html', plot_hatespeech_data_4chan=plot_hatespeech_data_4chan)
    return render_template('index.html', plot_hatespeech_data_4chan=None)

@app.route('/hatespeech_analysis_youtube', methods=['GET', 'POST'])
def hatespeech_youtube_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        hatespeech_df = hatespeech_youtube_query_database(start_date, end_date)
        plot_hatespeech_data_youtube = plot_hatespeech_analysis_youtube(hatespeech_df)
        return render_template('index.html', plot_hatespeech_data_youtube=plot_hatespeech_data_youtube)
    return render_template('index.html', plot_hatespeech_data_youtube=None)

@app.route('/hatespeech_analysis_politics', methods=['GET', 'POST'])
def hatespeech_politics_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        hatespeech_df = hatespeech_politics_query_database(start_date, end_date)
        plot_hatespeech_data_politics = plot_hatespeech_analysis_politics(hatespeech_df)
        return render_template('index.html', plot_hatespeech_data_politics=plot_hatespeech_data_politics)
    return render_template('index.html', plot_hatespeech_data_politics=None)

@app.route('/sentiment_analysis_reddit', methods=['GET', 'POST'])
def sentiment_reddit_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        sentiment_df = reddit_query_database(start_date, end_date)
        plot_data_reddit = plot_sentiment_analysis_reddit(sentiment_df)
        return render_template('index.html', plot_data_reddit=plot_data_reddit)
    return render_template('index.html', plot_data_reddit=None)

@app.route('/sentiment_analysis_4chan', methods=['GET', 'POST'])
def sentiment_4chan_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        sentiment_df = chan4_query_database(start_date, end_date)
        plot_data_4chan = plot_sentiment_analysis_4chan(sentiment_df)
        return render_template('index.html', plot_data_4chan=plot_data_4chan)
    return render_template('index.html', plot_data_4chan=None)

@app.route('/sentiment_analysis_youtube', methods=['GET', 'POST'])
def sentiment_youtube_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        sentiment_df = youtube_query_database(start_date, end_date)
        plot_data_youtube = plot_sentiment_analysis_youtube(sentiment_df)
        return render_template('index.html', plot_data_youtube=plot_data_youtube)
    return render_template('index.html', plot_data_youtube=None)


@app.route('/sentiment_analysis_politics', methods=['GET', 'POST'])
def sentiment_politics_route():
    if request.method == 'POST':
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        sentiment_df = politics_query_database(start_date, end_date)
        plot_data_politics = plot_sentiment_analysis_politics(sentiment_df)
        return render_template('index.html', plot_data_politics=plot_data_politics)
    return render_template('index.html', plot_data_politics=None)

if __name__ == '__main__':
    app.run(debug=True)

