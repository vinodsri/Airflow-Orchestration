# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests
import os

# You can override them on a per-task basis during operator initialization
default_args=1{
    'owner': 'John Doe',
    'start_date': days_ago(0) ,
    'email': ['john.doe@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data'
    default_args=default_args,
    description='Apache Airflow Final Assignment', 
    schedule_interval=timedelta (days=1),
)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    # Send a get request to the web url
    with requests.get(url, stream=true) as response:
        # Raise an exception for HTTP errors
        response.raise_for_status()
        # Open a local file in birnary write mode
        with open(input_file, 'wb') as f:
            # write the content to the local file in chunks
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"File download is successful: {input_file}")

def download_dataset{
    https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz /home/project/airflow/dags/python_etl/staging
};

# Define task unzip_data to unzip data
def untar_dataset (input_file):
    task_id='unzip-data'
    bash_command = f" cd (HOME_DIR] && tar-xvzf tolldata. tgz",
    dag=dag


def extract_data_from_csv():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def extract_data_from_tsv():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def extract_data_from_fixed_widthto():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def consolidate_data():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def format_data():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:
            processed_line = line.upper()
            outfile.write(processed_line + '\n')

def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')

def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)



# Define task extract_data_from_csv to extract fields from csv file
extract_data_from_csv = BashOperator (
    task_id='extract-csv',
    bash_command = f"cut -d',' -f1-4 {HOME_DIR)/vehicle-data.csv > {HOME_DIR}/csv_data.csv",
    dag=dag,


# Define task extract_data_from_tsv to extract fields from tsv file
    extract_data_from_tsv = BashOperator (
    task_id='extract-tsv',
    bash_command = f"cut -f5-7 {HOME_DIR}/tollplaza-data.tsv > {HOME_DIR}/tsv_data.csv",
    dag=dag,
)

# betine task extract_data_from_fixed _width to extract fields from fixed width file
extract_data_from_fixed_width = BashOperator (
    task_id='extract-fixed-width',
    bash_command = f"awk '{{print $10 |",\" $11}}' {HOME_DIR}/payment-data.txt > {HOME_DIR}/fixed_width_data.csv",
    dag=dag,
)

# Define task consolidate_data to unify data extracted from diffent files
consolidate_data = BashOperator (
    task_id='consolidate-data',
    bash_command = (f"paste -d',' "
                    f"{HOME_DIR}/csv_data.csv "
                    f"{HOME_DIR}/fixed_width_data.csv "
                    f"{HOME_DIR}/tsv_data.csv "
                    f"'> {HOME_DIR}/extracted_data.csv"
    ),
    dag=dag,
)

# Define task transform_data to capitalize vehicle type field value
transform_data = BashOperator(
task_id='transform-data',
bash_command = f"""awk -F',' -V OFS=',' '{{ $4 = toupper($4); print }}' {HOME_DIR}/extracted_data.csv > {STAGING_DIR}/transformed_data.csv""",
dag=dag,

# Task pipeline
unzip_data > extract_data_from_csv > extract_data_from_tsv > extract_data_from_fixed_width >> consolidate_data » transform_data