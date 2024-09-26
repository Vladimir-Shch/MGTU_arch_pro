# sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# sudo apt-get update
# sudo apt-get -y install postgresql

# /etc/postgresql/15/main/pg_hba.conf
# 
# host    all             all              0.0.0.0/0                       trust
# host    all             all              ::/0                            trust

# /etc/postgresql/15/main/postgresql.conf
# listen_addresses = '*'

# sudo pg_ctlcluster 15 main restart

# https://postgrespro.com/docs/postgrespro/10/demodb-bookings-installation.html
# wget https://edu.postgrespro.com/demo-big-en.zip
# sudo apt-get install unzip
# sudo unzip demo-big-en.zip -d ./datasets/
# sudo -u postgres psql -f demo-big-en-20170815.sql



import logging as _log

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import requests
import json
 
from typing import Optional
from typing import List
from pydantic import BaseModel, ValidationError

PG_CONN_ID = 'postgres_otus'
PG_TABLE = 'bitcoin_jokes'

URL_RESPONSE_BITCOIN = requests.get('https://api.coincap.io/v2/rates')
URL_RESPONSE_CHUCK_NORRIS = requests.get('https://api.chucknorris.io/jokes/random') 


# def conn_bitcoin():
#     try:
#         URL_RESPONSE_BITCOIN.raise_for_status()
#     except Exception as ex:
#         print(ex)
#     _log.info('connecting to bitcoin_rate api')


# def conn_chucknorris():
#     try:
#         url_response_chucknorris.raise_for_status()
#     except Exception as ex:
#         print(ex)

#     _log.info('connecting to chucknorris api')


def get_bitcoin_rate(**context):
    dict_of_values_bitcoin = json.dumps(URL_RESPONSE_BITCOIN.json())

    class Data_bitcoin(BaseModel):
        id: str
        symbol: str
        currencySymbol: Optional[str]
        rateUsd: str

    class JSON_bitcoin(BaseModel):
        data: List[Data_bitcoin]
        timestamp: int

    try:
        json_bitcoin = JSON_bitcoin.parse_raw(dict_of_values_bitcoin)
    except ValidationError as e:
        print("Exception",e.json())
    else: 
        for i in range(len(json_bitcoin.data)):
            if json_bitcoin.data[i].id == "bitcoin":           
                bitcoin_rate = json_bitcoin.data[i].rateUsd
                timestamp_msr = json_bitcoin.timestamp  

    # context["task_instance"].xcom_push(key="btc_json", value=(bitcoin_rate, timestamp_msr))
    # return bitcoin_rate, timestamp_msr


def get_chucknorris_joke(*context):
    dict_of_values_chucknorris = json.dumps(URL_RESPONSE_CHUCK_NORRIS.json())
    
    class ChucknorrisJoke(BaseModel):
        value: str    

    try:
        chucknorrisjoke = ChucknorrisJoke.parse_raw(dict_of_values_chucknorris)
    except ValidationError as e:
        print("Exception",e.json())
    else:         
        chucknorris_joke = chucknorrisjoke.value
    # return chucknorris_joke     
    # context["task_instance"].xcom_push(key="chuck_json", value=chucknorris_joke)


# def put_to_psql(**context):
#     population_string = """ CREATE TABLE IF NOT EXISTS {0} (BitcoinRate INT, ChuckNorrisJoke TEXT, Timestamp BIGINT, CurrentTime timestamp);
#                             INSERT INTO {0} 
#                             (BitcoinRate, ChuckNorrisJoke, Timestamp, CurrentTime) 
#                             VALUES ({1},$uniq_tAg${2}$uniq_tAg$,{3},current_timestamp);
#                         """ \
#                         .format(
#                             PG_TABLE, 
#                             context["task_instance"].xcom_pull(task_ids="get_bitcoin_rate", key="btc_json")[0], 
#                             context["task_instance"].xcom_pull(task_ids="parse_data", key="chuck_json"),
#                             context["task_instance"].xcom_pull(task_ids="get_bitcoin_rate", key="btc_json")[1]
#                         )
#     return population_string


args = {
    'owner': 'airflow',
    'catchup': 'False',
}

with DAG(
    dag_id='bitcoin_n_jokes_2pg',
    default_args=args,
    schedule_interval='*/30 * * * *',
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['4otus', 'API'],
    catchup=False,
) as dag:
    # task_connect_bitcoin = PythonOperator(
    #     task_id='connect_to_api_bitcoin', 
    #     python_callable=conn_bitcoin, 
    #     dag=dag
    # )
    # task_connect_chucknorris = PythonOperator(
    #     task_id='connect_to_api_chucknorris', 
    #     python_callable=conn_chucknorris, 
    #     dag=dag
    # )
    task_get_bitcoin_rate = PythonOperator(
        task_id='get_bitcoin_rate', 
        python_callable=get_bitcoin_rate, 
        provide_context=True,
        dag=dag
    )
    task_get_chucknorris_joke = PythonOperator(
        task_id='get_chucknorris_joke', 
        python_callable=get_chucknorris_joke, 
        provide_context=True,
        dag=dag
    )
    # task_populate = PostgresOperator(
    #     task_id="put_to_psql_bitcoin_rate_and_chucknorris_joke",
    #     postgres_conn_id=PG_CONN_ID,
    #     sql=put_to_psql(),
    #     provide_context=True,
    #     dag=dag
    # )

    # task_connect_bitcoin >> task_connect_chucknorris >> 
    task_get_bitcoin_rate >> task_get_chucknorris_joke 
    # >> task_populate
