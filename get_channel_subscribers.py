import os
import requests
import psycopg2
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi as yta
import asyncio

load_dotenv(".env.local")

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

postgres_url = os.getenv("POSTGRES_URL")

def get_subscribers(channel_id):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics&id={channel_id}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'items' in data and len(data['items']) > 0:
            subscribers = data['items'][0]['statistics']['subscriberCount']
            return int(subscribers)
        else:
            print(f"No data found for channel ID: {channel_id}")
            return None
    else:
        print(f"Failed to retrieve data for channel ID: {channel_id}, status code: {response.status_code}")
        return None

def batch_insert_subscribers(channel_ids, postgres_url):
    # Connect to PostgreSQL
    conn = psycopg2.connect(postgres_url)
    cursor = conn.cursor()

    # Prepare the records
    records = []
    for channel_id in channel_ids:
        try:
            subs = get_subscribers(channel_id)
        except Exception as e:
            print(f"Error getting transcript for {channel_id}: {e}")
            subs = None

        records.append((
            channel_id,
            subs
            ))

    # Insert into the database in batch
    query = '''
        INSERT INTO channel_subscribers (
            channel_id, subscribers
        )
        VALUES (%s, %s)
        ON CONFLICT (channel_id) DO UPDATE
        SET 
            subscribers = EXCLUDED.subscribers
    '''
    try:
        cursor.executemany(query, records)
        conn.commit()
        print(f"Inserted/Updated {cursor.rowcount} rows.")
    except Exception as e:
        print(f"Error during batch insert: {e}")
    finally:
        cursor.close()
        conn.close()

if postgres_url:
    try:
        conn = psycopg2.connect(postgres_url)
        print("Connection successful")

        cursor = conn.cursor()            
    except Exception as e:
        print(f"Connection failed: {e}")
else:
    print("PostgreSQL URL not found.")
if postgres_url:
    try:
        conn = psycopg2.connect(postgres_url)
        print("Connection successful")

        cursor = conn.cursor()            
    except Exception as e:
        print(f"Connection failed: {e}")
else:
    print("PostgreSQL URL not found.")

# Create a table to store YouTube transcripts

cursor.execute('''
CREATE TABLE IF NOT EXISTS channel_subscribers (
    channel_id TEXT PRIMARY KEY,
    subscribers integer
    )
''')

conn.commit()

print("Table created successfully.")

# Get the video IDs from the database
cursor.execute('''SELECT DISTINCT channel_id
FROM youtube_videos v
''')
channel_ids = cursor.fetchall()
channel_ids = [channel_id[0] for channel_id in channel_ids]

if channel_ids:
    batch_insert_subscribers(channel_ids, postgres_url)