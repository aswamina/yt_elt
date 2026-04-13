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
    

if __name__ == "__main__":
    load_dotenv(override=True)

    YOUTUBE_API_KEY=os.getenv("YOUTUBE_API_KEY")
    CHANNEL_HANDLE=os.getenv("CHANNEL_HANDLE")
    try:
        playlist_id = get_channel_playlist_id(CHANNEL_HANDLE, YOUTUBE_API_KEY)
        print(f"Channel Playlist ID: {playlist_id}")
    except Exception as e:
        print(f"An error occurred while fetching the channel playlist ID: {e}")