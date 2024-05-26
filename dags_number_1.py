import os
import requests
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


from transform_script import transform

DAG_ID = 'pavlova_dag'
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 4, 5, tz=pendulum.timezone("Europe/Moscow")),
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'description': 'ETL DAG for monthly calculation of customer activity based on transactions.',
    'max_active_runs': 1,
    'catchup': False,
}

# Используем функцию для загрузки и обработки данных
def download_and_process_data(date):
    url = 'https://drive.google.com/file/d/1Q0peDtX6oEWEHV5CxexvpHtC1rujiQ8X/view?usp=drive_link' # URL для загрузки данных
    data_dir = '/tmp/airflow/data/' 
    os.makedirs(data_dir, exist_ok=True)
    output_path = os.path.join(data_dir, f'profit_table_{date}.csv')

    response = requests.get(url)
    response.raise_for_status()

    with open(output_path, 'wb') as file:
        file.write(response.content)
    print(f"Файл успешно загружен и сохранен в {output_path}")

    df = pd.read_csv(output_path)
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    all_flags = [transform(df, date, product) for product in product_list]

    final_df = pd.concat(all_flags).drop_duplicates()
    final_df.to_csv(output_path, index=False)
    print(f"Данные успешно обработаны и сохранены в {output_path}")

with DAG(
    DAG_ID,
    default_args=default_args,
    description=default_args['description'],
    start_date=default_args['start_date'],
    schedule_interval='0 0 5 * *',
    catchup=default_args['catchup'],
    max_active_runs=default_args['max_active_runs']
) as dag:

    task = PythonOperator(
        task_id='download_and_process_data',
        python_callable=download_and_process_data,
        op_kwargs={'date': '{{ ds }}'},
    )