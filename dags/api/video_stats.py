import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime
from airflow.decorators import task


@task
def get_channel_playlist_id(channel_handle, api_key):

    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()

        channel_items = data["items"][0]
        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        return channel_playlist_id
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise e
    

def get_playlist_items(playlist_id, api_key, max_results=5, page_token=None):
    
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&playlistId={playlist_id}&key={api_key}&maxResults={max_results}"
        if page_token:
            url += f"&pageToken={page_token}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        raise e


@task
def get_video_ids(playlist_id=None, api_key=None, max_results=5):
    if not playlist_id:
        raise ValueError("playlist_id must be provided.")

    video_ids = []
    pageToken = None

    for _ in range(3):  # cleaner than while
        playlist_items = get_playlist_items(
            playlist_id, api_key, max_results, pageToken
        )

        if not playlist_items:
            raise ValueError("Empty response from YouTube API")

        if "error" in playlist_items:
            raise Exception(f"YouTube API error: {playlist_items['error']}")

        for item in playlist_items.get("items", []):
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                video_ids.append(str(video_id))

        pageToken = playlist_items.get("nextPageToken")

        if not pageToken:
            break

    return video_ids


def batch_video_ids(video_ids, batch_size=5):
    for i in range(0, len(video_ids), batch_size):
        yield video_ids[i:i + batch_size]


@task
def get_video_details(video_ids, api_key):
    video_details = []
    for batch in batch_video_ids(video_ids):
        ids = ",".join(batch)
        url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={ids}&key={api_key}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        for item in data.get("items", []):
            video_details.append({
                "videoId": item["id"],
                "title": item["snippet"]["title"],
                "description": item["snippet"]["description"],
                "publishedAt": item["snippet"]["publishedAt"],
                "channelTitle": item["snippet"]["channelTitle"],
                "statistics": item.get("statistics", {})
            })
    return video_details


@task
def save_to_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


if __name__ == "__main__":

    # Load environment variables from .env file
    load_dotenv(override=True)
    YOUTUBE_API_KEY=os.getenv("YOUTUBE_API_KEY")
    CHANNEL_HANDLE=os.getenv("CHANNEL_HANDLE")
    max_results=5

    try:
        playlist_id = get_channel_playlist_id(CHANNEL_HANDLE, YOUTUBE_API_KEY)
    except Exception as e:
        print(f"An error occurred while fetching the channel playlist ID: {e}")

    try:
        video_ids = get_video_ids(playlist_id, YOUTUBE_API_KEY, max_results)
        #print(json.dumps(video_ids, indent=4))
    except Exception as e:
        print(f"An error occurred while fetching the video IDs: {e}")

    try:
        video_details = get_video_details(video_ids, YOUTUBE_API_KEY)
        #print(json.dumps(video_details, indent=4))
    except Exception as e:
        print(f"An error occurred while fetching the video details: {e}")

    try:
        save_to_json(video_details, f"data/video_details_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json")
    except Exception as e:
        print(f"An error occurred while saving the video details to JSON: {e}")     