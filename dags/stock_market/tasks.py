from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException


bucket_name = 'stock-market'

def get_minio_client():
    minio = BaseHook.get_connection('minio_defualt')

    client= Minio (
        endpoint = minio.extra_dejson['endpoint_url'].split('//')[1] , 
        access_key = minio.login,
        secret_key = minio.password,
        secure = False
    )
    return client


def get_stock_prices(url , symbol):
    import requests
    import json
    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(response.json()['chart']['result'][0] )

def store_prices( stock):
    import json
    
    client = get_minio_client()
   
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol  = stock['meta']['symbol']
    data = json.dumps(stock , ensure_ascii=False ).encode('utf-8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{symbol}/prices.json",    
        data = BytesIO(data),
        length=len(data)
    )
    return f'{objw.bucket_name}/{symbol}'

def get_formated_prices(path):
    client = get_minio_client()
    prefix_name  = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(bucket_name, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException(f"No formatted prices found in {prefix_name}")    
            
    