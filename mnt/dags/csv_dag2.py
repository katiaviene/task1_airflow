from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime
import pandas as pd
import shutil
import json
import os


FILE = "/airflow/files/employee_data.csv"
ORIG_DIR = "/airflow/files"
NEW_DESTINATION = "/airflow/result"


def _check_file(file):
    return os.path.exists(file)

def _read_csv(file, ti):
    df = pd.read_csv(file)
    data_json = df.to_json()
    data_json_string = json.dumps(data_json)
    ti.xcom_push('empl_data', data_json_string)

def _write_csv(ti):
    read_data = ti.xcom_pull(task_ids="read_csv_task", key="empl_data")
    data = json.loads(read_data)
    df = pd.read_json(data)
    df.to_csv(NEW_DESTINATION + "/result2.csv")

def _relocate_csv(file, new_dest):
    shutil.move(file, new_dest)

with DAG("csv_read_2", start_date=datetime(2021, 1 ,1), 
    schedule_interval="*/15 * * * *", catchup=False) as dag:
    
    check_file_task = ShortCircuitOperator(
        task_id='check_file',
        python_callable=_check_file,
        op_args=[FILE,]
    )

    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=_read_csv,
        op_args=[FILE,],
      
    )

    write_csv_task = PythonOperator(
        task_id="write_csv_task",
        python_callable=_write_csv
    )

    relocate_csv = PythonOperator(
        task_id='relocate_csv',
        python_callable=_relocate_csv,
        op_args=[FILE, NEW_DESTINATION]
    )

    check_file_task >> read_csv_task >>  write_csv_task >> relocate_csv