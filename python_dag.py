import requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
def get_btc_data(**content):
    btc_result = requests.get('https://api.coincap.io/v2/rates/bitcoin')
    res_json = btc_result.json()
    base_row = {
       'deleted_sign': 0,
       'service_id': res_json['data']['id'] + '_' + str(res_json['timestamp'])
       }
    res_data= res_json['data']
    res_data.update(base_row)
    content['ti'].xcom_push(key= 'btc_json', value= res_data)

def parse_data(**content):
    btc_json= content['ti'].xcom_pull(key='btc_json')
    btc_full_data = [btc_json['id'], btc_json['symbol'], btc_json['currencySymbol'], btc_json['type'], btc_json['rateUsd'], btc_json['deleted_sign'], btc_json['service_id']]
    content['ti'].xcom_push(key='btc_full_push', value=btc_full_data)

def insert_data_hook(**content):
    btc_final_result = content['ti'].xcom_pull(key ='btc_full_push')
    request =f"INSERT INTO btc_table(id, symbol,currencySymbol, type,rateUsd,deleted_sign,service_id ) VALUES( '{btc_final_result[0]}','{btc_final_result[1]}',' {btc_final_result[2]}','{btc_final_result[3]}','{btc_final_result[4]}', '{btc_final_result[5]}','{btc_final_result[6]}')"
    pg_hook= PostgresHook(postgres_conn_id= 'postgres_airflow')
    print(request)
    connect= pg_hook.get_conn()
    curs=connect.cursor()
    curs.execute(request)
    connect.commit()


default_args = {'owner': 'd_masalov'}

with DAG('btc_load_dag', default_args= default_args, start_date = datetime(2021, 1, 1), schedule_interval = '*/30 * * * *') as dag:

    run_postgress_create_table =PostgresOperator(
        task_id= 'create_btc_table',
        postgres_conn_id= 'postgres_airflow',
        sql ='''CREATE TABLE IF NOT EXISTS btc_table (
                id varchar(50),
                symbol varchar(50),
                currencySymbol varchar(50),
                type varchar(50),
                rateUsd varchar(50),
                deleted_sign varchar(50),
                service_id varchar(50)); '''
         )
    run_postgres_hook= PythonOperator(task_id = 'insert_hgook_data', python_callable = insert_data_hook)
    run_get_btc_data= PythonOperator(task_id='get_btc_data', python_callable= get_btc_data )
    run_parse_btc_data = PythonOperator(task_id ='parse_btc_data', python_callable = parse_data)

    run_postgress_create_table >> run_get_btc_data >> run_parse_btc_data >> run_postgres_hook
~                                                                                                                                                                                                                                          ~                                                                                                                                                                                                                                          ~                                                                                                                                                                                                                                          ~                                                                                                                                                                                                                                          ~                                                                                                                                                                                                                                          "btc_dag.py" 54L, 2589C written