from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests


def print_welcome():
    print("Welcome to Airflow!")


def print_date():
    print(f"Today is {datetime.today().date()}")


def print_random_quote():
    try:
        response = requests.get("https://zenquotes.io/api/random", timeout=10)
        response.raise_for_status()
        quote = response.json()[0]["q"]
        print(f'Quote of the day: "{quote}"')
    except Exception as e:
        print(f"Error fetching quote: {e}")


with DAG(
    dag_id="welcome_dag",
    start_date= datetime(2026, 2, 27),
    schedule="@daily",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="print_welcome",
        python_callable=print_welcome,
    )

    task2 = PythonOperator(
        task_id="print_date",
        python_callable=print_date,
    )

    task3 = PythonOperator(
        task_id="print_random_quote",
        python_callable=print_random_quote,
    )

    task1 >> task2 >> task3