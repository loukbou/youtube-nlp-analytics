from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
from googleapiclient.discovery import build
import os
from airflow.models import Variable

# YouTube API Key
API_KEY = Variable.get("YOUTUBE_API_KEY")


CHANNEL_ID = 'UCoOae5nYA7VqaXzerajD0lg'

# Initialize YouTube API client
youtube = build("youtube", "v3", developerKey=API_KEY)

def scrape_youtube_data():
    """Fetch channel stats and video details from YouTube API"""

    # Fetch channel statistics
    request = youtube.channels().list(part="snippet,contentDetails,statistics", id=CHANNEL_ID)
    response = request.execute()
    
    channel_data = {
        'channelName': response['items'][0]['snippet']['title'],
        'subscribers': response['items'][0]['statistics']['subscriberCount'],
        'views': response['items'][0]['statistics']['viewCount'],
        'totalVideos': response['items'][0]['statistics']['videoCount'],
        'playlistId': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }

    # Get video IDs
    playlist_id = channel_data['playlistId']
    video_ids = []
    
    request = youtube.playlistItems().list(part="contentDetails", playlistId=playlist_id, maxResults=50)
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    # Get video details
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet,statistics,contentDetails", id=video_id)
        response = request.execute()
        
        if response['items']:
            video = response['items'][0]
            video_data.append({
                'video_id': video['id'],
                'title': video['snippet']['title'],
                'viewCount': video['statistics'].get('viewCount', 0),
                'likeCount': video['statistics'].get('likeCount', 0),
                'commentCount': video['statistics'].get('commentCount', 0),
                'publishedAt': video['snippet']['publishedAt']
            })

    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.DataFrame(video_data)
    df.to_csv(f"/opt/airflow/dags/data/scraped_data_{timestamp}.csv", index=False)

# Airflow DAG setup
default_args = {
    'owner': 'Bouchra',
    'start_date': datetime(2025, 3, 23),
}

Mydag = DAG(
    'scraping_dag',
    default_args=default_args,
    schedule_interval=None, 
)

scraping_task = PythonOperator(
    task_id='scrape_youtube',
    python_callable=scrape_youtube_data,
    dag=Mydag,
)

trigger_preprocessing = TriggerDagRunOperator(
    task_id="trigger_preprocessing_dag",
    trigger_dag_id="preprocessing_dag", 
    dag=Mydag,
)

scraping_task >> trigger_preprocessing
