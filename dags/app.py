
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import random

#1) fetch amazon data (extract) 2) clean data (transform)

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
}


import random

def get_amazon_data_books(num_books, ti):
    sample_titles = [
        "Data Engineering with Python", "Designing Data-Intensive Applications",
        "Streaming Systems", "The Data Warehouse Toolkit", "Building Data Pipelines",
        "Fundamentals of Data Engineering", "Modern Data Architecture", "Data Engineering for Beginners",
        "Data Pipelines Pocket Reference", "Cloud Data Engineering", "Big Data Processing",
        "Real-Time Data Processing", "The Art of Data Engineering", "ETL with Apache Airflow",
        "Mastering Spark", "Data Modeling for Data Warehouses", "SQL for Data Engineers",
        "The Data Engineer's Cookbook", "Google Cloud for Data Engineering", "AWS Data Engineering Guide",
        "Kafka for Data Engineers", "Data Engineering Projects", "DataOps in Practice",
        "Advanced Data Integration", "Snowflake Essentials", "Databricks in Action",
        "BigQuery Deep Dive", "Azure Data Engineering", "Efficient Data Loading Techniques",
        "Data Quality Fundamentals", "Data Lakes Demystified", "The Path to Data Mastery",
        "Data Infrastructure for Startups", "Airflow in Practice", "Python ETL Projects",
        "Data Engineering on the Cloud", "Effective Data Engineering", "Introduction to Data Mesh",
        "Data Governance for Engineers", "Building Scalable Data Systems", "Time-Series in Big Data",
        "Data Architect's Handbook", "Distributed Systems for Engineers", "Apache Flink in Practice",
        "SQL Performance Tuning", "Data Engineering in Finance", "Streaming Analytics",
        "Postgres for Data Engineers", "Data Ingestion Strategies", "Real-World ETL Workflows"
    ]
    
    books = []
    for i in range(num_books):
        books.append({
            "Title": sample_titles[i % len(sample_titles)] + f" Vol-{(i // len(sample_titles)) + 1}",
            "Author": f"Author {chr(65 + i % 26)}",
            "Price": str(random.randint(20, 100)),
            "Rating": f"{round(random.uniform(3.5, 5.0), 1)} out of 5"
        })
    
    print(f"Mock book data generated: {len(books)} records")
    ti.xcom_push(key='book_data', value=books)


#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task