import os
import requests
import psycopg2
from dotenv import load_dotenv

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
        if "items" in data and len(data["items"]) > 0:
            subscribers = data["items"][0]["statistics"]["subscriberCount"]
            return int(subscribers)
        else:
            print(f"No data found for channel ID: {channel_id}")
            return None
    else:
        print(
            f"Failed to retrieve data for channel ID: {channel_id}, status code: {response.status_code}"
        )
        return None


def batch_insert_subscribers(channel_ids, postgres_url):
    # Connect to PostgreSQL
    conn = psycopg2.connect(postgres_url)
    cursor = conn.cursor()

    # Prepare the records
    records = []
    skipped_count = 0
    for channel_id in channel_ids:
        try:
            subs = get_subscribers(channel_id)
        except Exception as e:
            print(f"Error getting subscribers for {channel_id}: {e}")
            subs = None

        # Skip updates for errors (None) or zero subscribers
        if subs is None:
            print(f"Skipping update for {channel_id}: API error or no data")
            skipped_count += 1
            continue
        elif subs == 0:
            print(f"Skipping update for {channel_id}: zero subscribers")
            skipped_count += 1
            continue

        records.append((channel_id, subs))

    print(f"Skipped {skipped_count} channels due to errors or zero subscribers")

    # Insert into the database in batch (only if we have valid records)
    if records:
        query = """
            INSERT INTO channel_subscribers (
                channel_id, subscribers
            )
            VALUES (%s, %s)
            ON CONFLICT (channel_id) DO UPDATE
            SET 
                subscribers = EXCLUDED.subscribers
        """
        try:
            cursor.executemany(query, records)
            conn.commit()
            print(f"Inserted/Updated {cursor.rowcount} rows.")
        except Exception as e:
            print(f"Error during batch insert: {e}")
    else:
        print("No valid records to insert - all channels were skipped")

    # Always clean up database connections
    cursor.close()
    conn.close()


def main():
    """Main function to orchestrate the subscriber fetching process."""
    if not postgres_url:
        print("PostgreSQL URL not found.")
        return

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(postgres_url)
        print("Connection successful")
        cursor = conn.cursor()

        # Create a table to store channel subscribers
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS channel_subscribers (
            channel_id TEXT PRIMARY KEY,
            subscribers integer
        )
        """
        )
        conn.commit()
        print("Table created successfully.")

        # Get the channel IDs from the database
        cursor.execute("""SELECT DISTINCT channel_id FROM youtube_videos""")
        channel_ids = cursor.fetchall()
        channel_ids = [channel_id[0] for channel_id in channel_ids]

        print(f"Found {len(channel_ids)} unique channel IDs")

        if channel_ids:
            batch_insert_subscribers(channel_ids, postgres_url)
        else:
            print("No channel IDs found in the database.")

    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        if "cursor" in locals():
            cursor.close()
        if "conn" in locals():
            conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()
