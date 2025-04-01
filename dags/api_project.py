import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

import requests
import os

def _get_youtube_data():
    # API_KEY = "api_key_form_youtube"
    # หรือใช้ .env หรือ Airflow Variable ก็ได้ เช่น:
    # API_KEY = os.environ.get("YOUTUBE_API_KEY")
    API_KEY = Variable.get("youtube_api_key")
    # https://www.youtube.com/watch?v=ic8j13piAhQ : cruel summer 
    video_id = "ic8j13piAhQ"
    url = "https://www.googleapis.com/youtube/v3/videos"
    payload = {
        "part": "snippet,statistics",
        "id": video_id,
        "key": API_KEY
    }

    response = requests.get(url, params=payload)
    print(response.url)  # ลองดูว่า request ที่ส่งหน้าตาเป็นไง

    data = response.json()
    print(data)

    title = data["items"][0]["snippet"]["title"]
    views = data["items"][0]["statistics"]["viewCount"]

    print(f"วิดีโอ: {title}")
    print(f"ยอดวิว: {views}")

    with open("/opt/airflow/dags/data.json", "w") as f:
        json.dump(data, f)

with DAG(
    "youtube_api_dag",
    # schedule="@hourly",
    schedule="0 */3 * * *", #รันทุก 3 ชม https://crontab.guru/#0_*/3_*_*_*
    start_date=timezone.datetime(2025, 3, 31),
    catchup=False,
    tags=["peat"],
):
    start = EmptyOperator(task_id="start")

    get_youtube_data = PythonOperator(
        task_id="get_youtube_data",
        python_callable=_get_youtube_data,
    )

    end = EmptyOperator(task_id="end")

    start >> get_youtube_data >> end
