import os
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


local_home_path = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
dataset_download_path = f'{local_home_path}/spotify-dataset'

default_args = {
    'owner': 'iamraphson',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG(
    'spotify-project-dag',
    default_args=default_args,
    description='DAG for spotify dataset',
    tags=['spotify'],
) as dag:
    install_pip_packages_task = BashOperator(
        task_id='install_pip_packages',
        bash_command='pip install --user kaggle'
    )

    pulldown_dataset_task = BashOperator(
        task_id='pulldown_dataset',
        bash_command=f'kaggle datasets download tonygordonjr/spotify-dataset-2023 --path {dataset_download_path} --unzip'
    )

    install_pip_packages_task.set_downstream(pulldown_dataset_task)