from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import shutil

FILE = "/airflow/files/employee_data.csv"
ORIG_DIR = "/airflow/files"
NEW_DESTINATION = "/airflow/result"

def read_csv(file):
    df = pd.read_csv(file)

def _relocate_csv(file, new_dest):
    shutil.copy(file, new_dest)

with DAG("csv_read", start_date=datetime(2021, 1 ,1), 
    schedule_interval="*/15 * * * *", catchup=False) as dag:
    
    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv,
        op_args=[FILE,],
      
    )

    relocate_csv = PythonOperator(
        task_id='relocate_csv',
        python_callable=_relocate_csv,
        op_args=[FILE, NEW_DESTINATION]
    )

    read_csv_task >>  relocate_csv