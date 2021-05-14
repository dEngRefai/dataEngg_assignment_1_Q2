import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
import pandas as pd
from pymongo import MongoClient
import psycopg2
from sqlalchemy import create_engine

default_args={
    'owner': 'refai',
    'start_date': dt.datetime(2021, 5, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with  DAG('postgressCSVJSONmongodbDAG',
        default_args=default_args,
        schedule_interval=timedelta(minutes=1),
        catchup=False
        ) as dag:


    getDATA = PythonOperator(task_id='QueryPostgreSQL', 
                             python_callable=queryPostgresql)
    CSVJson = PythonOperator(task_id='convertCSVtoJson', 
                             python_callable=CSVToJson)
    insertData = PythonOperator(task_id='InsertDataMongoDB', 
                             python_callable=insertMongoDB)

getDATA >> CSVJson >>  insertData   
   


def queryPostgresql():
    # define engine
    host="postgres_storage"
    database="testDB"
    user="me"
    password="1234"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    #pull table into dataframe
    DFpostgress=pd.read_sql("SELECT * FROM users2020" , engine);
    DFpostgress.to_csv('/opt/airflow/dags/data.csv')

def CSVToJson():
    df=pd.read_csv('/opt/airflow/dags/data.csv')
    for i,r in df.iterrows():
        print(r['name'])
    df.to_json('/opt/airflow/dags/datatojson.json', orient='records')

    
def insertMongoDB():
    client = MongoClient('mongo:27017', 
                        username='root',
                         password='example')
    ## Create DataBase
    db = client['shawerma']

    article = open('/opt/airflow/dags/datatojson.json', 'r')

    articles = db.random
    
    #push documents to collection
    result = articles.insert_one(article)
  
  
  
  
  
  
  
  
  
  
  