import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


default_args = {
    'owner': 'Shubham',
    'start_date': datetime(2025, 9, 25)
}

def get_data():
    import requests
    res = requests.get('https://randomuser.me/api/', verify=False)
    return res.json()['results'][0]

def format_data(res):
    import uuid
    data = {}
    data['id'] = str(uuid.uuid4())  # Generate UUID for Cassandra PRIMARY KEY
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{res['location']['street']['number']} {res['location']['street']['name']}, {res['location']['city']}, {res['location']['state']}, {res['location']['country']}, {res['location']['postcode']}"
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    res = get_data()
    formatted_data = format_data(res)
    from kafka import KafkaProducer
    import time
    import json
    import os
    from dotenv import load_dotenv
    load_dotenv() 

    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, max_block_ms=5000)

    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            formatted_data = format_data(res)
            producer.send('user_data', value=json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            continue

with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
