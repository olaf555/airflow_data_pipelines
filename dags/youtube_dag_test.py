import json
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from datetime import datetime  # ✅ เปลี่ยนตรงนี้

import requests

DAG_FOLDER = "/opt/airflow/dags"
VIDEO_ID = "ic8j13piAhQ"

def _get_youtube_data():
    # assert 1 == 2
    API_KEY = Variable.get("youtube_api_key")
    url = "https://www.googleapis.com/youtube/v3/videos"
    payload = {
        "part": "snippet,statistics",
        "id": VIDEO_ID,
        "key": API_KEY
    }
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    with open(f"{DAG_FOLDER}/youtube_data.json", "w") as f:
        json.dump(data, f)

def _validate_youtube_data():
    with open(f"{DAG_FOLDER}/youtube_data.json", "r") as f:
        data = json.load(f)

    items = data.get("items", [])
    assert len(items) > 0, "No video data found."

def _create_youtube_table():
    pg_hook = PostgresHook(postgres_conn_id="youtube_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS youtube_stats (
            video_id TEXT PRIMARY KEY,
            title TEXT,
            view_count BIGINT,
            updated_at TIMESTAMP
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_youtube_data_to_postgres(): 
    pg_hook = PostgresHook(postgres_conn_id="youtube_postgres_conn", schema="postgres")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(f"{DAG_FOLDER}/youtube_data.json", "r") as f:
        data = json.load(f)

    item = data["items"][0]
    video_id = item["id"]
    title = item["snippet"]["title"]
    view_count = int(item["statistics"]["viewCount"])
    updated_at = datetime.utcnow()  # ✅ ใช้ datetime แทน timezone

    sql = """
        INSERT INTO youtube_stats (video_id, title, view_count, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE SET
            title = EXCLUDED.title,
            view_count = EXCLUDED.view_count,
            updated_at = EXCLUDED.updated_at
    """
    cursor.execute(sql, (video_id, title, view_count, updated_at))
    connection.commit()

default_args = {
    "email": ["perapat.sor@thailife.com"],
    "retries":3
}

with DAG(
    "youtube_dag_test",  # ✅ ชื่อใหม่
    default_args=default_args,
    schedule="0 */3 * * *",
    start_date=timezone.datetime(2025, 4, 1),
    catchup=False,
    tags=["youtube", "test"],
):
    start = EmptyOperator(task_id="start")

    get_data = PythonOperator(task_id="get_youtube_data", python_callable=_get_youtube_data)
    validate = PythonOperator(task_id="validate_youtube_data", python_callable=_validate_youtube_data)
    create_table = PythonOperator(task_id="create_youtube_table", python_callable=_create_youtube_table)
    load_data = PythonOperator(task_id="load_youtube_data_to_postgres", python_callable=_load_youtube_data_to_postgres)
    end = EmptyOperator(task_id="end")

    start >> get_data >> validate >> load_data >> end
    start >> create_table >> load_data
