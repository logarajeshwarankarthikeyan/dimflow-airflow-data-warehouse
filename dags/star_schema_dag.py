from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
from datetime import datetime, timezone
from uuid import uuid4
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from scripts.star_schema.process_data import transform_city_data, transform_weather_data , transform_date_data
from scripts.star_schema.ddl_scripts import *


def task_fail_slack_alert(context):
    slack_msg = f"""
            :red_circle: Task Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Log Url*: {context.get('task_instance').log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification_on_failure',
        slack_webhook_conn_id='slack_connection',
        message=slack_msg,
        channel='#airflow-notifications',
        username='airflow-bot',
    )
    return failed_alert.execute(context=context)


def dag_success_slack_alert(context):
    slack_msg = f"""
            :large_green_circle: DAG Succeeded!
            *Dag*: {context.get('dag').dag_id}
            *Execution Time*: {context.get('execution_date')}
            """
    success_alert = SlackWebhookOperator(
        task_id='slack_notification_on_success',
        slack_webhook_conn_id='slack_connection',
        message=slack_msg,
        channel='#airflow-notifications',
        username='airflow-bot',
    )
    return success_alert.execute(context=context)




dag = DAG(
    'star_schema_dag',
    start_date=datetime(2025, 5, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=task_fail_slack_alert,
    on_success_callback=dag_success_slack_alert,
    )


check_api = HttpSensor(
    task_id='check_api',
    http_conn_id='api_connection',
    endpoint='data/2.5/weather',    
    request_params={'q': 'London', 'appid': Variable.get('API_KEY')},
    response_check=lambda response: 'London' in response.text,
    poke_interval=5,
    timeout=20,
    dag=dag,
    )

def extract_data(**context):
    cities = ['London', 'Paris', 'Berlin', 'Rome', 'Madrid', 'Moscow', 'Tokyo', 'Delhi', 'Beijing', 'New York']
    results = []
    output_path = '/opt/airflow/data/weather_data.json'
    api_key = Variable.get('API_KEY')
    for city in cities:
        response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}')
        if response.status_code == 200:
            results.append(response.json())
        else:
            results.append({'city': city, 'error': response.text})

    with open(output_path, 'w') as f:
        import json
        json.dump(results, f, indent=2)

    context['task_instance'].xcom_push(key='weather_data', value=output_path)

def transform_data(**context):
    weather_data_path = context['task_instance'].xcom_pull(task_ids='extract_task', key='weather_data')
    city_df = transform_city_data(weather_data_path)
    weather_df = transform_weather_data(weather_data_path)
    date_df = transform_date_data()
    context['task_instance'].xcom_push(key='city_data', value=city_df)
    context['task_instance'].xcom_push(key='weather_data', value=weather_df)
    context['task_instance'].xcom_push(key='date_data', value=date_df)

def load_data(**context):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    batch_id = str(uuid4())
    loaded_at_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    city_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='city_data')
    date_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='date_data')
    weather_data = context['task_instance'].xcom_pull(task_ids='transform_data', key='weather_data')

    try:
        cursor.execute(DROP_TABLES)
        cursor.execute(DIM_CITY)
        cursor.execute(DIM_DATE)
        cursor.execute(FACT_WEATHER)
        cursor.execute(STG_CITY)
        cursor.execute(STG_DATE)
        cursor.execute(STG_WEATHER)

        cursor.executemany(
            "INSERT INTO stg_city VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [(batch_id, loaded_at_utc, row['city_id'], row['city'], row['country_code'], row['latitude'], row['longitude']) for row in city_data],
        )
        cursor.executemany(
            "INSERT INTO stg_date VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [(batch_id, loaded_at_utc, row['date_id'], row['year'], row['month'], row['day'], row['day_of_week'], row['is_weekend']) for row in date_data],
        )
        cursor.executemany(
            "INSERT INTO stg_weather VALUES (%s, %s, %s, %s, %s, %s)",
            [(batch_id, loaded_at_utc, row['city_id'], row['date_id'], row['temperature'], row['humidity']) for row in weather_data],
        )

        cursor.execute("INSERT INTO dim_city (city_id, city_name, country_code, latitude, longitude) SELECT city_id, city_name, country_code, latitude, longitude FROM stg_city WHERE load_batch_id = %s", (batch_id,))
        cursor.execute("INSERT INTO dim_date (date_id, year, month, day, day_of_week, is_weekend) SELECT date_id, year, month, day, day_of_week, is_weekend FROM stg_date WHERE load_batch_id = %s", (batch_id,))
        cursor.execute("INSERT INTO fact_weather (city_id, date_id, temperature, humidity) SELECT city_id, date_id, temperature, humidity FROM stg_weather WHERE load_batch_id = %s", (batch_id,))
        conn.commit()
        context['task_instance'].xcom_push(
            key='load_metadata',
            value={
                'batch_id': batch_id,
                'loaded_at_utc': loaded_at_utc.isoformat(),
                'city_rows': len(city_data),
                'date_rows': len(date_data),
                'weather_rows': len(weather_data),
            },
        )
        print(f"Loaded batch {batch_id}: {len(city_data)} cities, {len(date_data)} dates, {len(weather_data)} weather records.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
    )

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
    )

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
    )

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
    )

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
    )


start_task >> check_api >> extract_task  >> transform_task >> load_task >> end_task
