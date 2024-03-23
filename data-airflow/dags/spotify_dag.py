import os
import logging
import datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_SPOTIFY_BUCKET = os.environ.get('GCP_SPOTIFY_BUCKET')
GCP_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

dataset_download_path = f'{AIRFLOW_HOME}/spotify-dataset/'

def do_clean_to_parquet(ti):
    pq_root_paths = []
    for filename in os.listdir(dataset_download_path):
        if filename.endswith('.csv'):
            dataset_df = pd.read_csv(f'{dataset_download_path}{filename}', low_memory=False)
            partition_cols = None
            parquet_root_path = dataset_download_path

            if filename.startswith('spotify-albums_data'):
                dataset_df['release_date'] = pd.to_datetime(dataset_df['release_date']).dt.date
                dataset_df = dataset_df.drop(columns=[
                    'artists',
                    'duration_ms',
                    'artist_7',
                    'artist_8',
                    'artist_9',
                    'artist_10',
                    'artist_11'
                ], axis='columns')
                partition_cols = ['album_type']
                parquet_root_path = f'{dataset_download_path}spotify_albums_pq'
            elif filename.startswith('spotify_artist_data'):
                dataset_df = dataset_df.drop(columns=[
                    'genre_5',
                    'genre_6'
                ], axis='columns')
                partition_cols = ['artist_popularity']
                parquet_root_path = f'{dataset_download_path}spotify_artist_pq'
            elif filename.startswith('spotify_features_data'):
                dataset_df['duration_sec'] = dataset_df['duration_ms'] / 1000
                dataset_df = dataset_df.drop(columns=[
                    'duration_ms',
                    'uri',
                    'analysis_url',
                    'track_href',
                    'type'
                ], axis='columns')
                parquet_root_path = f'{dataset_download_path}spotify_features_pq'
            elif filename.startswith('spotify_tracks_data'):
                partition_cols = ['explicit', 'track_popularity']
                parquet_root_path = f'{dataset_download_path}spotify_tracks_pq'
            else:
                continue


            dataset_table = pa.Table.from_pandas(dataset_df)
            pq.write_to_dataset(
                dataset_table,
                root_path=parquet_root_path,
                partition_cols=partition_cols
            )
            pq_root_paths.append(parquet_root_path)

    ti.xcom_push(key='pq_root_paths', value=pq_root_paths)

def do_upload_pq_to_gcs(ti):
    pq_root_paths = ti.xcom_pull(key='pq_root_paths', task_ids='do_clean_to_parquet_task')
    if pq_root_paths is None:
        pq_root_paths =[]

    pq_dirs = []
    gcs = pafs.GcsFileSystem()
    for dir in pq_root_paths:
        pq_dir = dir.split('/')[-1]

        gcs_path = f'{GCP_SPOTIFY_BUCKET}/{pq_dir}'
        dir_info = gcs.get_file_info(gcs_path)
        if dir_info.type == pafs.FileType.Directory:
            gcs.delete_dir(gcs_path)

        gcs.create_dir(gcs_path)

        pafs.copy_files(
            source=dir,
            destination=gcs_path,
            destination_filesystem=gcs
        )
        pq_dirs.append(pq_dir)

    logging.info('Copied parquet to gsc')
    ti.xcom_push(key='pq_root_paths', value=pq_dirs)

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

    do_clean_to_parquet_task = PythonOperator(
        task_id='do_clean_to_parquet_task',
        python_callable=do_clean_to_parquet
    )

    do_upload_pq_to_gcs_task = PythonOperator(
        task_id='do_upload_pq_to_gcs',
        python_callable=do_upload_pq_to_gcs
    )

    clean_task = BashOperator(
        task_id='clean_task',
        bash_command=f"rm -rf {dataset_download_path}"
    )

    install_pip_packages_task.set_downstream(pulldown_dataset_task)
    pulldown_dataset_task.set_downstream(do_clean_to_parquet_task)
    do_clean_to_parquet_task.set_downstream(do_upload_pq_to_gcs_task)
    do_upload_pq_to_gcs_task.set_downstream(clean_task)
