from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator  # ✅ Import ถูกต้อง

with DAG(
    "my_first_dag",
    start_date=timezone.datetime(2025, 2, 1),
    schedule=None,  # ✅ ใช้ schedule_interval แทน schedule
    tags=["dpu", "my_first_dag", "hello"],
) as dag:  # ✅ เปลี่ยน ; เป็น :
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2  # ✅ กำหนด dependency ให้ t1 รันก่อน t2
