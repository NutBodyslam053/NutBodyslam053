from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests

# Airflow connection for MySQL database
MYSQL_CONNECTION = "mysql_default"
# Web API data path
CONVERSION_RATE_URL = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

mysql_output_path = "/home/airflow/gcs/data/audible_data_merged.csv"
api_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"

def get_data_from_mysql(mysql_output_path):
    # Establish a connection to a MySQL database using an Airflow connection
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query some data from a MySQL database
    audible_data = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_data")
    audible_transaction = mysqlserver.get_pandas_df(sql="SELECT * FROM audible_transaction")

    # Merge queried data
    df = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    # Save the data as a CSV file in /home/airflow/gcs/data, and it will be automatically stored in gs://bucket-name/data as well
    df.to_csv(mysql_output_path, index=False)
    print(f"Output to {mysql_output_path}")

def get_conversion_rate(api_output_path):
    # Retrieve conversion rate data from a web API
    r = requests.get(CONVERSION_RATE_URL)
    result_conversion_rate = r.json()
    df = pd.DataFrame(result_conversion_rate)

    # Save the data as a CSV file in /home/airflow/gcs/data, and it will be automatically stored in gs://bucket-name/data as well
    df = df.reset_index().rename(columns={"index": "date"})
    df.to_csv(api_output_path, index=False)
    print(f"Output to {api_output_path}")

def merge_data(mysql_output_path, api_output_path, final_output_path):
    # Read data
    transaction = pd.read_csv(mysql_output_path)
    conversion_rate = pd.read_csv(api_output_path)
    
    # Transform data
    transaction['date'] = transaction['timestamp']
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # Merge data
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")
    
    # Manipulate data
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
    final_df["Price"] = final_df["Price"].astype(float)
    final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
    final_df = final_df.drop(["date", "book_id"], axis=1)

    # Save the data as a CSV file in /home/airflow/gcs/data, and it will be automatically stored in gs://bucket-name/data as well
    final_df.to_csv(final_output_path, index=False)
    print(f"Output to {final_output_path}")

with DAG(
    "exercise4_final_dag",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["workshop"]
) as dag:

    dag.doc_md = """
        Extract data from a MySQL database and a web API, perform data transformations, 
        and store it to the Google Cloud Storage (GCS).
    """

    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={"mysql_output_path": mysql_output_path},
    )

    t2 = PythonOperator(
        task_id="get_conversion_rate",
        python_callable=get_conversion_rate,
        op_kwargs={"api_output_path": api_output_path},
    )

    t3 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        op_kwargs={
            "mysql_output_path": mysql_output_path,
            "api_output_path": api_output_path,
            "final_output_path": final_output_path
        }
    )


[t1, t2] >> t3


# Create Cloud Composer on GCP
# Install PyPI packages; pymysql, requests, pandas
# Open Airflow web-UI; set up MySQL connections
# gsutil cp start_ws4_exercise4.py gs://asia-east2-datawarehouse-eab5d086-bucket/dags