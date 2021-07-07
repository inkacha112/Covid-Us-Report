from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pymysql.cursors
import pandas as pd
import requests
 

def covid_api():
   url = "https://api.covidtracking.com/v1/us/daily.json"
   response = requests.get(url)
   covid_data = response.json()
   covid_us = pd.DataFrame.from_dict(covid_data)
   covid_us = covid_us.drop(columns=['hash', 'total','posNeg','lastModified','dateChecked','deathIncrease','hospitalized']) # Drop Deprecated columns.
   covid_us['date'] = pd.to_datetime(covid_us['date'], format = "%Y%m%d")
   covid_us.to_csv("/home/airflow/gcs/data/covid_us.csv", index=False)

default_args = {
    'owner': 'datath',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once'
}

dag = DAG(
    'covid_pipeline',
    default_args=default_args,
    description='Pipeline for covid data',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='covid_api',
    python_callable=covid_api,
    dag=dag,
)

t2 = BashOperator(
    task_id='bq_load',
    bash_command='bq load --source_format=CSV --autodetect \
            covid_data.covid_us \
            gs://asia-east2-covidcase-thaila-b542307f-bucket/data/covid_us.csv',
    dag=dag,
)

t3 = EmailOperator(
        task_id='send_email',
        to=['wo.inkacha_st@tni.ac.th.team'],
        subject='Your COVID-19 report is ready',
        html_content='ingest data success,it now in your big query. :)',
    dag=dag,
)

t1 >> t2 >> t3