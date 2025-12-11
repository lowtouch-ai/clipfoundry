from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import time
import logging

def random_sleep_and_outcome(task_id: str):
    sleep_time = random.randint(3, 10)
    logging.info(f"[{task_id}] Sleeping for {sleep_time} seconds...")

    for i in range(sleep_time):
        logging.info(f"[{task_id}] {i+1}/{sleep_time} seconds passed...")
        time.sleep(1)

    outcome = random.choice(["fail", "return", "continue"])
    logging.info(f"[{task_id}] Woke up, outcome = {outcome}")

    if outcome == "fail":
        raise Exception(f"{task_id} failed intentionally")
    elif outcome == "return":
        return f"{task_id} completed successfully"
    else:
        # continue with None
        return None

with DAG(
    dag_id="clipfoundry_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aiops", "agent"],
) as dag:

    tasks = []

    for i in range(1, 6):  # 5 tasks
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=random_sleep_and_outcome,
            op_kwargs={"task_id": f"task_{i}"},
        )
        tasks.append(task)

    # Chain tasks sequentially
    tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> tasks[4]

