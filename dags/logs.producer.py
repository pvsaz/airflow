from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
from confluent_kafka import Producer
from airflow.sdk import Variable
import datetime
import logging
import boto3
fake = Faker()
logger = logging.getLogger(__name__)
import random
#from datetime import datetime
def create_kafka_producer(config):
    return Producer(config)

def generate_log():
    """Generate synthetic log"""
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    endpoints = ['/api/users', '/home', '/about', '/contact', '/services']
    statuses = [200, 301, 302, 400, 404, 500]

    user_agents = [
        'Mozilla/5.0 (iPhone; CPU IPhone OS 14_6 like Mac OS X)',
        'Mozilla/5.0 (X11; Linux x86_64)'
    ]

    referrers = ['https://example.com', 'https://google.com']

    ip = fake.ipv4()
    timestamp = datetime.datetime.now().strftime('%b %d %Y, %H:%M:%S')
    method = random.choice(methods)
    endpoint = random.choice(endpoints)
    status = random.choice(statuses)
    size = random.randint(1000, 15000)
    referrer = random.choice(referrers)
    user_agent = random.choice(user_agents)

    log_entry = (
        f'{ip} - - [{timestamp}] "{method} {endpoint} HTTP/1.1" {status} {size} "{referrer}" {user_agent}'
    )

    return log_entry

def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def get_secret(secret_name, region_name='us-east-1'):
    """Retrieve secrets from AWS secret manager"""
    session = boto3.session.Session()
    client = session.client(service_name = 'secretsmanager', region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retrieval error: {e}")
        raise

def produce_logs(**context):
    """ Produce log entries into Kafka"""
    #secrets = get_secret('MWAA_Secrets_V2')
    secrets = dict()
    secrets['KAFKA_BOOTSTRAP_SERVER'] = Variable.get('KAFKA_BOOTSTRAP_SERVER')
    secrets['KAFKA_SASL_PASSWORD'] = Variable.get('KAFKA_SASL_PASSWORD')
    secrets['KAFKA_SASL_USERNAME'] = Variable.get('KAFKA_SASL_USERNAME')
    kafka_config={
        'bootstrap.servers': secrets['KAFKA_BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        'session.timeout.ms': 5000,
        'sasl.mechanisms':   'PLAIN',
    }

    producer = create_kafka_producer(kafka_config)
    topic = 'billion_website_logs'

    for _ in range(150):
        log = generate_log()
        try:
            producer.produce(topic, log.encode('utf-8'), on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            logger.error(f'Error producing log: {e}')
            raise

    logger.info(f'Produced 15,000 logs to topic {topic}')

default_args = {
    'owner': 'Data Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
}

dag = DAG(
    'log_generation_pipeline',
    default_args=default_args,
    description='Generate and produce synthetic logs',
    schedule_interval='*/5 * * * *',
    start_date=datetime.datetime(2025, 6, 4),
    catchup=False,
    tags=['logs', 'kafka', 'production']
)

produce_logs_task = PythonOperator(
    task_id='generate_and_produce_logs',
    python_callable=produce_logs,
    dag=dag
)
#produce_logs()