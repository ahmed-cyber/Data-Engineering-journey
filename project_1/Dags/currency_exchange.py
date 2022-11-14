from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.bash import BashOperator
import os
import pandas as pd
import json


API_request = """
       wget --quiet \
	--method GET \
	--header 'X-RapidAPI-Key: 486c4e9873msh6a738483e6d9b6cp11c12fjsn4dfa27393ff2' \
	--header 'X-RapidAPI-Host: currency-conversion-and-exchange-rates.p.rapidapi.com' \
	--output-document \
	- 'https://currency-conversion-and-exchange-rates.p.rapidapi.com/latest' > /home/ahmed/currencyex.json 
    
    
       wget --quiet \
    --method GET \
    --header 'X-RapidAPI-Key: 486c4e9873msh6a738483e6d9b6cp11c12fjsn4dfa27393ff2' \
    --header 'X-RapidAPI-Host: currency-conversion-and-exchange-rates.p.rapidapi.com' \
    --output-document \
    - https://currency-conversion-and-exchange-rates.p.rapidapi.com/symbols > /home/ahmed/currencySy.json  """


def data_processing():
       curr_df = pd.read_json('/home/ahmed/currencyex.json')
       symb_df = pd.read_json('/home/ahmed/currencySy.json')
       curr_df['Currency'] = curr_df.index
       curr_df.reset_index(drop=True , inplace=True)
       #print(curr_df.shape)
       symb_df['Currency'] = symb_df.index
       symb_df.reset_index(drop=True , inplace=True)
       #print(symb_df.shape)
       final_df = curr_df.merge(symb_df)[['symbols','Currency','rates','timestamp','base']]
       final_df.to_csv('/home/ahmed/final.csv',index=False)


with DAG( dag_id='currency_exchange_dag',
schedule_interval='@hourly',
start_date=datetime(year=2022, month=11, day=8),
 catchup=False) as dag:

    get_data_task = BashOperator(task_id = 'get_data_task',
                                 bash_command =API_request
                                 )


    data_transformation_task = PythonOperator(
                                   task_id = 'data_transformation_task',
                                   python_callable=data_processing
                                   )



    create_table_task = PostgresOperator(task_id='create_table_task',
                                        postgres_conn_id='currdb',
                                        sql="""DROP TABLE IF EXISTS curr_ex;
                                                CREATE TABLE  curr_ex
                                                (
                                                    symbols varchar(255) ,
                                                    currency varchar(10),
                                                    rates float,
                                                    timestamp timestamp,
                                                    base varchar(10)
                                                )
                                                TABLESPACE pg_default;
                                                """)

    loading_data_task = BashOperator(task_id='loading_data_task',
                                      bash_command=(
                                                        'PGPASSWORD=YOUR-PASSWORD psql -d currdb -U YOUR-USER -h localhost -c "'
                                                        '\copy curr_ex(symbols, currency, rates,timestamp,base)'
                                                        "FROM '/home/ahmed/final.csv' "
                                                        "DELIMITER ',' "
                                                        'CSV HEADER"'
                                                    ))


get_data_task >>  data_transformation_task >>  loading_data_task  << create_table_task
 