import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add the root parent directory from the current file to sys.path so that we can import the modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import (  # noqa: E402
    extract_wikipedia_data,
    transform_wikipedia_data,
)

dag = DAG(
    dag_id="wikipedia_flow",
    default_args={
        "owner": "rosa pham",
        "start_date": datetime(2024, 8, 1),
    },
    schedule_interval=None,
    catchup=False,
)

# Extract data from Wikipedia
wiki_data = PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity",
    },
    dag=dag,
)

# Transform data extractec from Wikipedia
transform_data = PythonOperator(
    task_id="transform_wikipedia_data",
    python_callable=transform_wikipedia_data,
    provide_context=True,
    dag=dag,
)

# Load data into database
