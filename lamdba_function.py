import json
import pandas as pd
import time
import requests
import random
import time
import boto3
import os
from datetime import datetime

s3_client = boto3.client('s3')

def get_stats(api_key,channel_id):
    
    url_channel_stats = 'https://youtube.googleapis.com/youtube/v3/channels?part=statistics&id='+channel_id+'&key='+api_key
    channel_stats = requests.get(url_channel_stats).json()
    channel_stats = channel_stats['items'][0]['statistics']
    date = pd.to_datetime('today').strftime("%Y-%m-%d")

    data_channel = {

        'Created_at':date,
        'Total_Views':int(float(channel_stats['viewCount'])),
        'Subscribers':int(float(channel_stats['subscriberCount'])),
        'Video_count':int(float(channel_stats['videoCount'])),

    }
    
    return data_channel
    
def channel_stats(df,api_key):
    
    date = []
    views = []
    subscriber = []
    video_count = []
    channel_name = []
    
    time_wait = [1, 2, 3]
    
    for i in range(len(df)):
        stats_temp = get_stats(api_key, df['channel_id'][i])
        
        date.append(stats_temp['Created_at'])
        views.append(stats_temp['Total_Views'])
        subscriber.append(stats_temp['Subscribers'])
        video_count.append(stats_temp['Video_count'])
        channel_name.append(df['channel_name'][i])
        
        time.sleep(random.choice(time_wait))
        
    data = {
 
        'Channel_name': channel_name,
        'Subscribers': subscriber,
        'Video_count': video_count,
        'Total_Views': views,
        'created_at': date
    }
        
    df_channels_final = pd.DataFrame(data)
    
    return df_channels_final    

def lambda_handler(event, context):
    
    bucket_name = os.environ['BUCKET_INPUT']
    filename =  os.environ['FILE_CHANNELS']
    developer_key = os.environ['APIKEY']
    
    # Get File from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key= filename)
    df_channels = pd.read_csv(obj['Body']) # 'Body' is a key word
    
    
    results = channel_stats(df_channels, developer_key)
    #date = pd.to_datetime('today').strftime("%Y%m%d")
    
    timestamp = int(time.time())
    
    results.to_csv(f'/tmp/youtube_stats_{timestamp}.csv',index = False)
 
    # Send file to  S3
    s3 = boto3.resource("s3")
    
    s3.Bucket(os.environ['BUCKET_DESTINY']).upload_file(f'/tmp/youtube_stats_{timestamp}.csv', Key=f'youtube_stats_{timestamp}.csv')
    os.remove(f'/tmp/youtube_stats_{timestamp}.csv')

    return f'file youtube_stats_{timestamp}.csv send succeded'
    
