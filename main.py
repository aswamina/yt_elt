import os
import requests
import json
from dotenv import load_dotenv


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



def get_video_ids(playlist_id = None, api_key=None, max_results=5):
    video_ids = []
    pageToken = None
    count = 0

    while count < 5: # Limit to 5 pages of results

        if playlist_id:
            playlist_items = get_playlist_items(playlist_id, api_key, max_results, pageToken)

            for item in playlist_items.get("items", []):
                video_ids.append(item["contentDetails"]["videoId"])

            pageToken = playlist_items.get("nextPageToken")
        else:
            raise ValueError("Either playlist_id or video_url must be provided.")
        
        if not pageToken:
            break
        
        count += 1

    return video_ids


if __name__ == "__main__":
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
        print(json.dumps(video_ids, indent=4))
    except Exception as e:
        print(f"An error occurred while fetching the video IDs: {e}")

    