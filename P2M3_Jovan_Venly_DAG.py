import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def queryPostgresql():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_Jovan_Venly_data_raw.csv', index=False)
    print("-------Data Saved------")

def cleanAirbnb():
    data = pd.read_csv('/opt/airflow/dags/P2M3_Jovan_Venly_data_raw.csv')
    data = data.drop_duplicates()
    data['price'].fillna(data['price'].median(), inplace=True)
    data['mileage'].fillna(data['mileage'].median(), inplace=True)
    mode_fill_cols = ['engine', 'fuel', 'transmission', 'body', 'doors', 'exterior color', 'interior color']
    for col in mode_fill_cols:
        data[col].fillna(data[col].mode()[0], inplace=True)
    data['cylinders'].fillna(data['cylinders'].mode()[0], inplace=True)

    num_cols = data.select_dtypes(include=['number']).columns
    cat_cols = data.select_dtypes(include=['object']).columns
    null_cols = data.columns[data.isnull().any()]

    for i in num_cols:
        data[i].fillna(data[i].median(), inplace=True)
    for i in cat_cols:
        data[i].fillna(data[i].mode()[0], inplace=True)
    for i in null_cols:
        data[i] = data[i].fillna('No Data')

    data.dropna(axis=0, inplace=True)
    data.columns = [x.lower() for x in data.columns]
    data.columns = data.columns.str.replace(' ', '_')
    data.to_csv('/opt/airflow/dags/P2M3_Jovan_Venly_data_cleaned.csv')

def insertElasticsearch():
    es = Elasticsearch() 
    df = pd.read_csv('/opt/airflow/dags/P2M3_Jovan_Venly_data_cleaned.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="frompostgresql", doc_type="_doc", body=doc)
        print(res)

default_args = {
    'owner': 'jovan',
    'start_date': dt.datetime(2024, 7, 23),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('p2m3',
         default_args=default_args,
         schedule_interval='30 6 * * *',
         catchup=False) as dag:
    
    # Initial task to display a message in the terminal.
    node_start = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now..."'
    )

    Fetch_from_Postgresql = PythonOperator(
        task_id='queryPostgresql',
        python_callable=queryPostgresql
    )
    
    Data_Cleaning = PythonOperator(
        task_id='cleanAirbnb',
        python_callable=cleanAirbnb
    )

    Post_to_Elasticsearch = PythonOperator(
        task_id='insertElasticsearch',
        python_callable=insertElasticsearch
    )

    node_start >> Fetch_from_Postgresql >> Data_Cleaning >> Post_to_Elasticsearch

