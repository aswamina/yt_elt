from airflow import DAG
import pendulum
from datetime import date, datetime, timedelta
from api.video_stats import get_channel_playlist_id, get_video_ids, save_to_json, get_video_details
from airflow.models import Variable
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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

# Variables
staging_schema = "staging"
core_schema = "core"

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

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db",
    )

    # Task dependencies
    playlist_id >> video_ids >> video_details >> save_to_json_task >> trigger_update_db
    

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


    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )

    # Define dependencies
    update_staging >> update_core >> trigger_data_quality


# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core