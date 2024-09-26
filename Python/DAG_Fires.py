import requests
#import urllib2
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


def _request_data(**context):
    url = 'http://api.open-notify.org/iss-now.json'
    payload = {}
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    req_btc = requests.request("GET", url, headers=headers, data=payload)
    btc_json = req_btc.json()['iss_position']
    context["task_instance"].xcom_push(key="btc_json", value=btc_json)


def _parse_data(**context):
    btc_json = context["task_instance"].xcom_pull(
        task_ids="request_data", key="btc_json")
    # ? should we really parse 'symbol'?
    btc_data = [btc_json['latitude'],  btc_json['longitude']]
    context["task_instance"].xcom_push(key="btc_data", value=btc_data)


def _insert_data(**context):
    btc_data = context["task_instance"].xcom_pull(
        task_ids="parse_data", key="btc_data")
    print('test SVI 1')
    print(btc_data)
    print('test SVI 2')
    dest = PostgresHook(postgres_conn_id='postgres_otus')
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()
    dest_cursor.execute(
        """INSERT INTO btc_data(latitude,  longitude) VALUES (%s, %s)""", (btc_data))
    dest_conn.commit()


args = {'owner': 'airflow'}

dag = DAG(
    dag_id="A_Space_Station",
    default_args=args,
    description='DAG ISS - Hausaufgabe 4',
    start_date=datetime(2023, 7, 24),
    schedule_interval='*/15 * * * *',
    tags=['4otus', 'DZ4'],
    catchup=False,
)


start = DummyOperator(task_id="start", dag=dag)

request_data = PythonOperator(
    task_id='request_data', python_callable=_request_data, provide_context=True, dag=dag)
parse_data = PythonOperator(
    task_id='parse_data', python_callable=_parse_data, provide_context=True, dag=dag)
insert_data = PythonOperator(
    task_id='insert_data', python_callable=_insert_data, provide_context=True, dag=dag)

create_table_if_not_exist = PostgresOperator(
    task_id='create_table_if_not_exist',
    postgres_conn_id="postgres_otus",
    sql='''CREATE TABLE IF NOT EXISTS btc_data(
            date_load timestamp NOT NULL DEFAULT NOW()::timestamp,
            latitude varchar(100) NOT NULL,
            longitude varchar(100) NOT NULL
        );''', dag=dag
)


start >> create_table_if_not_exist >> request_data >> parse_data >> insert_data