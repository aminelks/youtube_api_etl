from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import configparser
import pandas as pd
import logging
import time
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow import DAG
import pandas as pd
import snowflake.connector
import numpy as np
from snowflake.connector.pandas_tools import write_pandas

config = configparser.ConfigParser()
config_file='/opt/airflow/dags/config.ini'
config.read(config_file)
youtube = build('youtube', 'v3', developerKey=config['CREDS']['dev_key'])
search_term=config['YOUTUBE']['search_term']


def save_last_processed_date_and_token(last_processed_date, next_page_token=None):
    if next_page_token is not None:
        next_page_token = str(next_page_token)
    else:
        next_page_token = ''
    last_processed_date=last_processed_date.strftime("%Y-%m-%d")
    config.set('YOUTUBE', 'next_page_token', next_page_token) 
    config.set('YOUTUBE', 'last_processed_date', last_processed_date)
    with open(config_file, 'w') as configfile:
        config.write(configfile)

def load_last_processed_date_and_token():
    if config.has_option('YOUTUBE', 'last_processed_date'):
        last_processed_date=config.get('YOUTUBE', 'last_processed_date')
    else:
        last_processed_date="2022-01-01"
    if config.has_option('YOUTUBE', 'next_page_token'):
        next_page_token=config.get('YOUTUBE', 'next_page_token')
        if next_page_token=='':
            next_page_token=None
    else:
        next_page_token=None
    return last_processed_date,next_page_token

def save_to_csv(df):
    current_date = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = f'/opt/airflow/dags/data/Youtube_Data_{search_term}_{current_date}.csv'
    df.to_csv(output_path, index=False)
    
def get_research_vids_by_date(search_term,published_after, published_before,next_page_token):
    try:
        request = youtube.search().list(
        part="snippet",
        q=search_term,
        type="video",
        maxResults=50,
        publishedAfter=published_after.strftime("%Y-%m-%dT00:00:00Z"),
        publishedBefore=published_before.strftime("%Y-%m-%dT00:00:00Z"),
        pageToken=next_page_token)
        response = request.execute()
        video_ids = [item['id']['videoId'] for item in response['items']]
        
        video_request = youtube.videos().list(
            part="snippet,statistics",
            id=','.join(video_ids)
            )
        video_response = video_request.execute()
        return video_response,response.get('nextPageToken')
    except HttpError as e:            
        if e.resp.status == 403 and 'quotaExceeded' in str(e):
            logging.error("Daily quota exceeded. Stopping execution for today.")
        else:
            logging.error(f"Error during API call: {e}")
        return None, next_page_token
        
def processing(response,data):
    if response is None:
        return
    
    for item in response['items']:
        video_id = item['id']
        title = item['snippet']['title']
        channel_id = item['snippet']['channelId']
        channel_title = item['snippet']['channelTitle']
        language = item['snippet'].get('defaultLanguage','N/A')
        publish_date = item['snippet']['publishedAt']
        view_count = item['statistics'].get('viewCount', np.nan)
        like_count = item['statistics'].get('likeCount', np.nan)
        comment_count = item['statistics'].get('commentCount', np.nan)
        
        data.append({
            'Video ID': video_id,
            'Title': title,
            'Channel ID': channel_id,
            'Channel Title': channel_title,
            'Language': language,
            'Publish Date': publish_date,
            'View Count': view_count,
            'Like Count': like_count,
            'Comment Count': comment_count
            })


def fetch_videos_by_dates(search_term, start_date, end_date, next_page_token):
    data = []
    i = 0
    logging.info(f"Fetching videos from {start_date} to {end_date}")

    while True:
        try:
            response, next_page_token = get_research_vids_by_date(search_term, start_date, end_date, next_page_token)
            if response is None:
                logging.error("No response received, stopping fetch for this date range.")
                return data, next_page_token   

            processing(response, data)

            if next_page_token is None:
                logging.info(f'Page {i+1} successfully processed for date range: {start_date} to {end_date}')
                logging.info(f"All pages processed for date range: {start_date} to {end_date}")
                break

            logging.info(f'Page {i+1} successfully processed for date range: {start_date} to {end_date}')
            logging.info(f"Waiting 20 seconds before next request...")
            time.sleep(20)
            i += 1
        except HttpError as e:
            logging.error(f"Error during processing: {e}. Waiting 2 minutes before retry...")
            time.sleep(120)
            continue
    
    logging.info(f"{len(data)} videos processed for date range: {start_date} to {end_date}")
    return data, next_page_token


@task
def extract_videos_data():
    videos_data=[]
    days_delta = timedelta(days=4)

    start_date_str, last_processed_token = load_last_processed_date_and_token()
    start_date=datetime.strptime(start_date_str,"%Y-%m-%d")
    current_date = datetime.now()
    while start_date < current_date: 
        end_date=start_date+days_delta
        logging.info(f"Searching videos from: {start_date} to {end_date}")
        
        try:
            videos, next_page_token = fetch_videos_by_dates(search_term, start_date, end_date, last_processed_token)
            videos_data.extend(videos)

            if next_page_token or (len(videos)==0 and next_page_token is None):
                # Means quota was exceeded!
                logging.info(f"Quota reached. Saving progress for next run: {start_date} to {end_date}")
                break

            else:
                # All the pages for this daterange were processed
                start_date=end_date+timedelta(days=1)
                logging.info("Completed processing. Moving to next date range")
        
        except Exception as e:
            logging.error(f"Error encountered: {e}. Saving progress and stopping execution.")
            break

    logging.info(f"Successfully processed all data up to {start_date}")
    save_last_processed_date_and_token(start_date,next_page_token)
    if len(videos_data)>0:
        videos_df=pd.DataFrame(videos_data)
        videos_df['Publish Date']=pd.to_datetime(videos_df['Publish Date'])
        videos_df['Language']=videos_df['Language'].str.split('-').str[0]
        save_to_csv(videos_df)
        return videos_df
    
    

@task
def load_data(df):
    if df is not None:
        df['Publish Date'] = pd.to_datetime(df['Publish Date']).dt.date
        conn = snowflake.connector.connect(
        user=config['SNOWFLAKE']['user'],
        password=config['SNOWFLAKE']['password'],
        account=config['SNOWFLAKE']['account'],
        warehouse=config['SNOWFLAKE']['warehouse'],
        database=config['SNOWFLAKE']['database'],
        schema=config['SNOWFLAKE']['schema'],
        role=config['SNOWFLAKE']['role']
        )
        cur = conn.cursor()

        create_table_query ="""
        CREATE TABLE IF NOT EXISTS "youtube_data" (
        "Video ID" VARCHAR PRIMARY KEY,
        "Title" VARCHAR,
        "Channel ID" VARCHAR,
        "Channel Title" VARCHAR,
        "Language" VARCHAR,
        "Publish Date" DATE,
        "View Count" INTEGER,
        "Like Count" INTEGER,
        "Comment Count" INTEGER 
        );
        """
        cur.execute(create_table_query)
        logging.info('Table created or already exists.')

        success, num_chunks, num_rows, output = write_pandas(conn, df, 'youtube_data')

        if success:
            logging.info(f"{num_rows} rows successfully inserted into 'data'")
        else:
            logging.warning("Failed to insert data.")

        cur.close()
        conn.close()
        logging.info("Connection to Snowflake closed.")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'tags': ['etl_pg']
}

with DAG(
    dag_id='youtube_etl',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    src_data=extract_videos_data()
    load_data(src_data)
