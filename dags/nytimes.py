import requests
from airflow.decorators import dag, task
import datetime
import pendulum
import json
import pandas as pd
import s3fs
from helper.etl import extract_and_save_to_s3, load_parquet_to_stg_tbl
from helper.sql_queries import SQLQueries
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="nytimes_news",
    start_date=pendulum.datetime(2023, 12, 20, tz="local"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def process_nytimes_news():

    @task
    def extract():
        extract_and_save_to_s3()
    
    @task
    def load_stg_tbl():
        load_parquet_to_stg_tbl()
    
    @task
    def upsert_mio():
        query = SQLQueries.upsert_query
        try:
            postgres_hook = PostgresHook(postgres_conn_id="nytimes_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1
    
    @task
    def del_stg_table():
        query = SQLQueries.delete_stg_table_query
        try:
            postgres_hook = PostgresHook(postgres_conn_id="nytimes_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1
        
    extract() >> load_stg_tbl() >> upsert_mio() >> del_stg_table()

dag = process_nytimes_news()
         