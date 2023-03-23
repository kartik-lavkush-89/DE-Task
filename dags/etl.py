from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys

# Add the root directory to the system path so that Python can find the helper module
sys.path.insert(0, "/home/uslsz0807/Documents/DE_task")
from components.helper import extractData, transformData, loadData

# Set default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 3, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "0 12 * * *",
    "catchup": False,
}

# Define the DAG with the given arguments
dag = DAG("ETL", description="DE Task", default_args=default_args)

# Define the task to extract the employee data
extract_operator = PythonOperator(
    task_id="extract_employee_data", python_callable=extractData, dag=dag
)

# Define the task to transform the employee data
transform_employee_data_operator = PythonOperator(
    task_id="transform_employee_data", python_callable=transformData, dag=dag
)

# Define the task to load the employee data
load_employee_data_operator = PythonOperator(
    task_id="load_employee_data", python_callable=loadData, dag=dag
)

# Set the dependencies between the tasks
extract_operator >> transform_employee_data_operator >> load_employee_data_operator
