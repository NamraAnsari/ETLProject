from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table, trunc_table

from dataquality.soda import yt_elt_data_quality

localtz = pendulum.timezone("Europe/Malta")
staging_schema = "staging"
core_schema = "core"

default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "namra.sellergize@gmail.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1,tzinfo=localtz)
} 

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag:

    playlist_id = get_playlist_id()
    video_id = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_id)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_id >> extract_data >> save_to_json_task

with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into both stage & core schema',
    schedule='0 15 * * *',
    catchup=False
) as dag_update:
    
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core

with DAG(
    dag_id='truncate_db',
    default_args=default_args,
    description='DAG to truncate corrupted file in db',
    schedule=None,
    catchup=False
) as dag_trunc:
    truncate_table = trunc_table()

    truncate_table

with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    schedule='0 16 * * *',
    catchup=False
) as dag_quality:
    
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    soda_validate_staging >> soda_validate_core
