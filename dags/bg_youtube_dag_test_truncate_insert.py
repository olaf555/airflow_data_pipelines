# import json
# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils import timezone
# from datetime import datetime

# import requests

# DAG_FOLDER = "/opt/airflow/dags"
# VIDEO_ID = "ic8j13piAhQ"

# def _get_youtube_data():
#     API_KEY = Variable.get("youtube_api_key")
#     url = "https://www.googleapis.com/youtube/v3/videos"
#     payload = {
#         "part": "snippet,statistics",
#         "id": VIDEO_ID,
#         "key": API_KEY
#     }
#     response = requests.get(url, params=payload)
#     print(response.url)

#     data = response.json()
#     print(data)

#     with open(f"{DAG_FOLDER}/youtube_data.json", "w") as f:
#         json.dump(data, f)

# def _validate_youtube_data():
#     with open(f"{DAG_FOLDER}/youtube_data.json", "r") as f:
#         data = json.load(f)

#     items = data.get("items", [])
#     assert len(items) > 0, "No video data found."

# def _validate_view_count_range():
#     with open(f"{DAG_FOLDER}/youtube_data.json", "r") as f:
#         data = json.load(f)

#     view_count = int(data.get("items")[0].get("statistics", {}).get("viewCount", 0))
#     assert view_count >= 1000, "view_count too low"
#     # assert view_count <= 10_000_000, "view_count too high"

# def _create_youtube_table():
#     pg_hook = PostgresHook(postgres_conn_id="youtube_postgres_conn", schema="postgres")
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()

#     sql = """
#         CREATE TABLE IF NOT EXISTS youtube_stats (
#             video_id TEXT PRIMARY KEY,
#             title TEXT,
#             description TEXT,
#             channel_title TEXT,
#             published_at TIMESTAMP,
#             view_count BIGINT,
#             like_count BIGINT,
#             comment_count BIGINT,
#             category_id TEXT,
#             tags TEXT,
#             updated_at TIMESTAMP
#         )
#     """
#     cursor.execute(sql)
#     connection.commit()

# def _load_youtube_data_to_postgres():
#     pg_hook = PostgresHook(postgres_conn_id="youtube_postgres_conn", schema="postgres")
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()

#     with open(f"{DAG_FOLDER}/youtube_data.json", "r") as f:
#         data = json.load(f)

#     item = data["items"][0]
#     snippet = item["snippet"]
#     statistics = item["statistics"]

#     video_id = item["id"]
#     title = snippet.get("title")
#     description = snippet.get("description")
#     channel_title = snippet.get("channelTitle")
#     published_at = snippet.get("publishedAt")
#     view_count = int(statistics.get("viewCount", 0))
#     like_count = int(statistics.get("likeCount", 0))
#     comment_count = int(statistics.get("commentCount", 0))
#     category_id = snippet.get("categoryId", "")
#     tags = ",".join(snippet.get("tags", [])) if snippet.get("tags") else ""
#     updated_at = datetime.utcnow()

#     sql = """
#         INSERT INTO youtube_stats (
#             video_id, title, description, channel_title, published_at,
#             view_count, like_count, comment_count, category_id, tags, updated_at
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ON CONFLICT (video_id) DO UPDATE SET
#             title = EXCLUDED.title,
#             description = EXCLUDED.description,
#             channel_title = EXCLUDED.channel_title,
#             published_at = EXCLUDED.published_at,
#             view_count = EXCLUDED.view_count,
#             like_count = EXCLUDED.like_count,
#             comment_count = EXCLUDED.comment_count,
#             category_id = EXCLUDED.category_id,
#             tags = EXCLUDED.tags,
#             updated_at = EXCLUDED.updated_at
#     """
#     cursor.execute(sql, (
#         video_id, title, description, channel_title, published_at,
#         view_count, like_count, comment_count, category_id, tags, updated_at
#     ))
#     connection.commit()

# default_args = {
#     "email": ["perapat.sor@thailife.com"],
#     "retries": 3
# }

# with DAG(
#     "youtube_dag_test",
#     default_args=default_args,
#     schedule="0 */3 * * *",
#     start_date=timezone.datetime(2025, 4, 1),
#     catchup=False,
#     tags=["youtube", "test"],
# ):
#     start = EmptyOperator(task_id="start")

#     get_data = PythonOperator(task_id="get_youtube_data", python_callable=_get_youtube_data)
#     validate = PythonOperator(task_id="validate_youtube_data", python_callable=_validate_youtube_data)
#     validate_range = PythonOperator(task_id="validate_view_count_range", python_callable=_validate_view_count_range)
#     create_table = PythonOperator(task_id="create_youtube_table", python_callable=_create_youtube_table)
#     load_data = PythonOperator(task_id="load_youtube_data_to_postgres", python_callable=_load_youtube_data_to_postgres)
#     end = EmptyOperator(task_id="end")

#     start >> get_data >> [validate >> validate_range] >> load_data >> end
#     start >> create_table >> load_data
