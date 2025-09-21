import os
import requests
import psycopg2
import psycopg2.extras
import asyncio
import aiohttp
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional, Dict
from dotenv import load_dotenv

load_dotenv(".env")

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

postgres_url = os.getenv("POSTGRES_URL")

# Constants for optimization
MAX_CHANNELS_PER_REQUEST = 50  # YouTube API limit
MAX_CONCURRENT_REQUESTS = 10  # Avoid rate limiting
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1  # seconds


class RateLimiter:
    """Simple rate limiter to avoid hitting API quotas."""

    def __init__(self, max_requests: int = 100, time_window: int = 100):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []

    async def acquire(self):
        now = time.time()
        # Remove old requests outside the time window
        self.requests = [
            req_time for req_time in self.requests if now - req_time < self.time_window
        ]

        if len(self.requests) >= self.max_requests:
            # Wait until we can make another request
            sleep_time = self.time_window - (now - self.requests[0]) + 0.1
            await asyncio.sleep(sleep_time)
            return await self.acquire()

        self.requests.append(now)


async def get_subscribers_batch(
    session: aiohttp.ClientSession, channel_ids: List[str], rate_limiter: RateLimiter
) -> Dict[str, Optional[int]]:
    """Fetch subscriber counts for multiple channels in a single API call."""
    await rate_limiter.acquire()

    # Join up to 50 channel IDs for batch request
    channel_ids_str = ",".join(channel_ids[:MAX_CHANNELS_PER_REQUEST])
    url = f"https://www.googleapis.com/youtube/v3/channels"
    params = {"part": "statistics", "id": channel_ids_str, "key": YOUTUBE_API_KEY}

    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    result = {}

                    # Process successful responses
                    if "items" in data:
                        for item in data["items"]:
                            channel_id = item["id"]
                            subscribers = int(item["statistics"]["subscriberCount"])
                            result[channel_id] = subscribers

                    # Mark missing channels as None (not found)
                    for channel_id in channel_ids:
                        if channel_id not in result:
                            result[channel_id] = None

                    return result

                elif response.status == 403:
                    print(f"API quota exceeded. Status: {response.status}")
                    break
                elif response.status == 429:
                    # Rate limited, wait longer
                    wait_time = (2**attempt) * RETRY_DELAY
                    print(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait_time)
                else:
                    print(
                        f"API error {response.status} for channels: {channel_ids_str[:50]}..."
                    )
                    break

        except Exception as e:
            print(f"Exception during API call (attempt {attempt + 1}): {e}")
            if attempt < RETRY_ATTEMPTS - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))

    # Return None for all channels if all attempts failed
    return {channel_id: None for channel_id in channel_ids}


async def process_channels_concurrent(
    channel_ids: List[str],
) -> List[Tuple[str, Optional[int]]]:
    """Process all channels concurrently with rate limiting."""
    print(f"Processing {len(channel_ids)} channels with concurrent requests...")

    rate_limiter = RateLimiter()
    results = []

    # Create chunks of channels for batch processing
    channel_chunks = [
        channel_ids[i : i + MAX_CHANNELS_PER_REQUEST]
        for i in range(0, len(channel_ids), MAX_CHANNELS_PER_REQUEST)
    ]

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def process_chunk(chunk):
            async with semaphore:
                return await get_subscribers_batch(session, chunk, rate_limiter)

        # Process chunks concurrently
        tasks = [process_chunk(chunk) for chunk in channel_chunks]

        # Process with progress tracking
        completed = 0
        total_chunks = len(tasks)

        for task in asyncio.as_completed(tasks):
            chunk_result = await task
            completed += 1

            # Convert dict results to list of tuples
            for channel_id, subs in chunk_result.items():
                results.append((channel_id, subs))

            print(
                f"Progress: {completed}/{total_chunks} chunks completed "
                f"({completed/total_chunks*100:.1f}%)"
            )

    return results


def batch_insert_subscribers(channel_ids: List[str], postgres_url: str):
    """Optimized function to fetch and insert subscriber data."""
    print(f"Starting optimized processing for {len(channel_ids)} channels...")
    start_time = time.time()

    # Use asyncio to process channels concurrently
    try:
        if os.name == "nt":  # Windows
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        results = asyncio.run(process_channels_concurrent(channel_ids))
    except Exception as e:
        print(f"Error during concurrent processing: {e}")
        return

    # Process results and prepare database records
    records = []
    skipped_count = 0

    for channel_id, subs in results:
        # Skip updates for errors (None) or zero subscribers
        if subs is None:
            skipped_count += 1
            continue
        elif subs == 0:
            print(f"Skipping update for {channel_id}: zero subscribers")
            skipped_count += 1
            continue

        records.append((channel_id, subs))

    processing_time = time.time() - start_time
    print(f"API processing completed in {processing_time:.2f}s")
    print(f"Valid records: {len(records)}, Skipped: {skipped_count}")

    # Optimized database insertion
    if records:
        _insert_records_optimized(records, postgres_url)
    else:
        print("No valid records to insert - all channels were skipped")


def _insert_records_optimized(records: List[Tuple[str, int]], postgres_url: str):
    """Optimized database insertion with better error handling."""
    try:
        # Use connection with optimized settings
        conn = psycopg2.connect(
            postgres_url, cursor_factory=psycopg2.extras.RealDictCursor
        )

        # Optimize connection for bulk operations
        conn.autocommit = False

        with conn:
            with conn.cursor() as cursor:
                # Simple upsert without timestamp columns
                query = """
                    INSERT INTO channel_subscribers (
                        channel_id, subscribers
                    )
                    VALUES %s
                    ON CONFLICT (channel_id) DO UPDATE
                    SET 
                        subscribers = EXCLUDED.subscribers
                """

                # Use execute_values for better performance
                psycopg2.extras.execute_values(
                    cursor,
                    query,
                    records,
                    template=None,
                    page_size=1000,  # Process in chunks of 1000
                )

                print(f"Successfully inserted/updated {len(records)} records")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if "conn" in locals():
            conn.close()


def get_all_channel_ids(postgres_url: str) -> List[str]:
    """Get all unique channel IDs from the database."""
    try:
        conn = psycopg2.connect(postgres_url)
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT DISTINCT channel_id FROM youtube_videos")
                results = cursor.fetchall()
                return [row[0] for row in results]

    except Exception as e:
        print(f"Error getting channel IDs: {e}")
        return []
    finally:
        if "conn" in locals():
            conn.close()


def main():
    """Optimized main function for daily subscriber updates."""
    if not postgres_url:
        print("PostgreSQL URL not found.")
        return

    if not YOUTUBE_API_KEY:
        print("YouTube API key not found.")
        return

    total_start_time = time.time()

    try:
        # Connect to PostgreSQL and setup table
        conn = psycopg2.connect(postgres_url)
        print("Database connection successful")

        with conn:
            with conn.cursor() as cursor:
                # Create simple table (matches your existing structure)
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS channel_subscribers (
                        channel_id TEXT PRIMARY KEY,
                        subscribers INTEGER
                    )
                """
                )

                print("Table created successfully.")

        # Get all channels for daily update
        print("Getting all channels for daily update...")
        channel_ids = get_all_channel_ids(postgres_url)

        print(f"Found {len(channel_ids)} channels to update")

        if channel_ids:
            # Process in batches if we have a large number of channels
            batch_size = 1000  # Process 1000 channels at a time

            if len(channel_ids) > batch_size:
                print(f"Processing channels in batches of {batch_size}...")

                for i in range(0, len(channel_ids), batch_size):
                    batch = channel_ids[i : i + batch_size]
                    print(
                        f"\nProcessing batch {i//batch_size + 1}/{(len(channel_ids)-1)//batch_size + 1}"
                    )
                    batch_insert_subscribers(batch, postgres_url)

                    # Brief pause between batches to be API-friendly
                    if i + batch_size < len(channel_ids):
                        print("Pausing 5 seconds between batches...")
                        time.sleep(5)
            else:
                batch_insert_subscribers(channel_ids, postgres_url)
        else:
            print("No channels found in database.")

    except Exception as e:
        print(f"Database connection failed: {e}")

    finally:
        total_time = time.time() - total_start_time
        print(f"\nTotal execution time: {total_time:.2f}s")
        print("Process completed.")


if __name__ == "__main__":
    main()
