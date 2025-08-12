from airflow.decorators import dag   , task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from stock_market.tasks import get_stock_prices , store_prices , get_formated_prices , bucket_name
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

from airflow.providers.slack.notifications.slack import SlackNotifier
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata  
from sqlalchemy import Column, String, Float, INT

from datetime import datetime
SYMBOL = 'NVDA'

@dag(
    dag_id="stock_market",
    start_date = datetime(2025, 8, 5),
    schedule_interval='@daily',
    tags = ['stock_market'],
    catchup = False ,
    on_success_callback=SlackNotifier(
        slack_conn_id='Slack',
        text='Stock Market DAG completed successfully!'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='Slack',  
        text='Stock Market DAG failed!'
    )
    catchup = False ,
    on_success_callback=SlackNotifier(
        slack_conn_id='Slack',
        text='Stock Market DAG completed successfully!'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id='Slack',  
        text='Stock Market DAG failed!'
    )
)

def stock_market():
    
    @task.sensor(poke_interval=60, timeout=300 , mode = 'poke')
   
    def is_api_available () ->PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url , headers= api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value = url)
    
    get_stock_prices_task = PythonOperator(
        task_id='get_stock_prices',
        python_callable= get_stock_prices,
        op_kwargs={'url':'{{ti.xcom_pull(task_ids="is_api_available")}}', 'symbol': SYMBOL},
    )
    
    store_prices_task = PythonOperator(
        task_id='store_prices',
        python_callable= store_prices ,
        op_kwargs={'stock':'{{ti.xcom_pull(task_ids="get_stock_prices")}}'}
        
    )
    
    format_prices = DockerOperator(
        task_id = 'format_prices' ,
        image = 'airflow/stock-app' ,
        container_name = 'stock_prices_formatter',
        api_version = 'auto',
        auto_remove = 'success',
        docker_url='tcp://docker-proxy:2375',
        

        #network_mode = 'container:spark-master',
        network_mode = 'stock_pipeline_net',
        do_xcom_push=True,
        tty = True,
        xcom_all = False,
        mount_tmp_dir = False,
        environment = {
            'SPARK_APPLICATION_ARGS': '{{ti.xcom_pull(task_ids="store_prices")}}',
        } 
    )
    
    get_formated_csv = PythonOperator(
        task_id='get_formated_csv',
        python_callable = get_formated_prices,
        op_kwargs= {
            'path': '{{ti.xcom_pull(task_ids="store_prices")}}'
        }
        
    )
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file = File(
            path = f"s3://{bucket_name}/{{{{ti.xcom_pull(task_ids='get_formated_csv') }}}}",
            conn_id = 'minio_defualt'
        ),
        output_table = Table(
            name = 'stock_prices',
            metadata = Metadata(schema='public'),
            conn_id = 'postgres',
            columns=[
                Column('timestamp', INT),
                Column('close', Float),
                Column('high', Float),
                Column('low', Float),
                Column('open', Float),
                Column('volume', Float),
                Column('date', String),
            ],
        ),
        load_options={
            "aws_access_key_id":BaseHook.get_connection('minio_defualt').login,
            "aws_secret_access_key":BaseHook.get_connection('minio_defualt').password,
            "endpoint_url": BaseHook.get_connection('minio_defualt').extra_dejson['endpoint_url']
        }
    )
    
    is_api_available() >> get_stock_prices_task   >> store_prices_task >> format_prices >> get_formated_csv >> load_to_dw
        


stock_market()