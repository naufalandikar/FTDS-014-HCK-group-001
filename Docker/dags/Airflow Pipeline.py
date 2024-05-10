#load Libarry
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd
from datetime import timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

    
#Function for Get Table from SQL to CSV
def load_csv_to_postgres():
    '''
This function is utilized to upload CSV data obtained from a data source and store it into PostgreSQL.
    '''
    #database, username, and password for accesing postgreSQL server
    database = "finalproject"
    username = "finalproject"
    password = "finalproject"
    host = "postgres"

    # Creating connection to PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL when creating a SQLAlchemy connection
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()
    
    #read CSV 
    df = pd.read_csv('/opt/airflow/dags/fashion.csv')
    #saving csv into postgreSQL
    df.to_sql('raw_table', conn, index=False, if_exists='replace')  # M


def ambil_data():
    '''
    This function is used to connect Airflow processes to PostgreSQL. It requires the database name, username, password, and host.
    '''
    # fetch data
    database = "finalproject"
    username = "finalproject"
    password = "finalproject"
    host = "postgres"

    # Make URL to connect Postgree
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL when establishing a SQLAlchemy connection.
    engine = create_engine(postgres_url)
    conn = engine.connect()

    #saving CSV from postgre database
    df = pd.read_sql_query("select * from raw_table", conn)
    df.to_csv('/opt/airflow/dags/fashion.csv', sep=',', index=False)
    

#Preprocessing column to clean the dataset
def preprocessing(): 
    ''' 
    The function to clean data involves several steps. It includes dropping null and duplicate values, and changing data types as needed.
    '''
    # data loading
    data = pd.read_csv("/opt/airflow/dags/fashion.csv")
    
    #drop null and duplicates
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    #data saving
    data.to_csv('/opt/airflow/dags/clean_fashion.csv', index=False)
    
def load_csv_to_postgres2():
    '''
This function is utilized to upload CSV data obtained from a data source and store it into PostgreSQL.
    '''
    database = "finalproject"
    username = "finalproject"
    password = "finalproject"
    host = "postgres"

    # Creating connection to PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use this URL when creating a SQLAlchemy connection
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()
    #saving CSV from postgre database
    df = pd.read_csv('/opt/airflow/dags/clean_fashion.csv')
    #df.to_sql(nama_table, conn, index=False, if_exists='replace')
    df.to_sql('cust_table', conn, index=False, if_exists='replace')

#Upload Clean CSV to ElasticSearch   
def upload_to_elasticsearch():
    ''' 
    This function serves to upload a CSV file to the Elasticsearch system.
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/clean_fashion.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="cust_table", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        
#Setting        
default_args = {
    'owner': 'Naufal', 
    'start_date': datetime(2024, 2, 22, 17, 00)- timedelta(hours=7)
}

#Workflow
with DAG(
    "FinalProject_HCK14",
    description='FinalProject',
    schedule_interval='* * 1 * *', #airflow will run every 1st of every month
    default_args=default_args, 
    catchup=False
) as dag:
    
        # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) 
    
    # task: 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)
    
    # Task : 4
    load_clean_csv_task = PythonOperator(
        task_id='load_csv_to_postgres2',
        python_callable=load_csv_to_postgres2) 

    # Task: 5
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #The process to execute it in Airflow 
    load_csv_task >> ambil_data_pg >> edit_data >> load_clean_csv_task >> upload_data



