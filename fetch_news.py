import pandas as pd
import json
import requests
import datetime
from datetime import date
import uuid
import os
from google.cloud import storage

def upload_to_gcs(bucket_name, destination_blob_name, source_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"✅ File '{source_file_name}' uploaded to GCS path: '{destination_blob_name}'.")

def fetch_news_data():
    today = date.today()
    api_key = 'd132a68ed7ca4bd88ab40ad267714e34'

    start_date_value = str(today - datetime.timedelta(days=1))
    end_date_value = str(today)

    url_extractor = f"https://newsapi.org/v2/everything?q=apple&from={start_date_value}&to={end_date_value}&sortBy=popularity&apiKey={api_key}"
    print(f"🔍 Fetching data from: {url_extractor}")

    response = requests.get(url_extractor)
    try:
        d = response.json()
    except Exception as e:
        raise ValueError(f"❌ Failed to parse JSON response: {e}")

    # Debugging: print raw response
    print("🟡 Raw NewsAPI Response:")
    print(json.dumps(d, indent=2))

    if 'articles' not in d:
        raise ValueError(f"❌ 'articles' key not found in NewsAPI response. Response content: {d}")

    if not d['articles']:
        raise ValueError("⚠️ No articles found for the given date range.")

    df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

    for i in d['articles']:
        newsTitle = i.get('title', '')
        timestamp = i.get('publishedAt', '')
        url_source = i.get('url', '')
        source = i.get('source', {}).get('name', '')
        author = i.get('author', '')
        urlToImage = i.get('urlToImage', '')
        partial_content = i.get('content', '') or ''

        # Trim content to max 200 chars or end of last sentence
        if '.' in partial_content:
            trimmed_part = partial_content[:partial_content.rindex('.') + 1]
        else:
            trimmed_part = partial_content[:200]

        new_row = pd.DataFrame({
            'newsTitle': [newsTitle],
            'timestamp': [timestamp],
            'url_source': [url_source],
            'content': [trimmed_part],
            'source': [source],
            'author': [author],
            'urlToImage': [urlToImage]
        })

        df = pd.concat([df, new_row], ignore_index=True)

    # Save and upload
    current_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'run_{current_time}.parquet'
    print("📁 Writing DataFrame to:", filename)
    
    print("📂 Current Working Directory:", os.getcwd())
    df.to_parquet(filename)

    # Upload to GCS
    bucket_name = 'snowflake_project_test'
    destination_blob_name = f'news_data_analysis/parquet_files/{filename}'
    upload_to_gcs(bucket_name, destination_blob_name, filename)

    # Remove local file
    os.remove(filename)
    print(f"🗑️ Local file '{filename}' deleted after upload.")
