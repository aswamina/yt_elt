from airflow import DAG
import pendulum
from datetime import date, datetime, timedelta
from api.video_stats import get_channel_playlist_id, get_video_ids, save_to_json, get_video_details
from airflow.models import Variable
from datawarehouse.dwh import staging_table, core_table



YOUTUBE_API_KEY=Variable.get("YOUTUBE_API_KEY")
CHANNEL_HANDLE=Variable.get("CHANNEL_HANDLE")
max_results=5

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': pendulum.datetime(2026, 4, 12, tz="UTC"),
}

with DAG(
    dag_id='youtube_channel_video_stats',
    default_args=default_args,
    description='A DAG to fetch video stats from a YouTube channel and save to JSON',
    schedule_interval=timedelta(days=1),
) as dag:

    YOUTUBE_API_KEY = Variable.get("YOUTUBE_API_KEY")
    CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")

    playlist_id = get_channel_playlist_id(CHANNEL_HANDLE, YOUTUBE_API_KEY)
    video_ids = get_video_ids(playlist_id, YOUTUBE_API_KEY, max_results)
    video_details = get_video_details(video_ids, YOUTUBE_API_KEY)
    save_to_json_task = save_to_json(video_details, f"data/video_details_{date.today()}.json")

    # Task dependencies
    playlist_id >> video_ids >> video_details >> save_to_json_task
    

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule=None,
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # Define dependencies
    update_staging >> update_core 