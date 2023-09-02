# import the libraries
# File location: airflow/dags directory for it to be picked up by airflow.

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# The file path from airflow-docker container's perspective.
dags_dir = "/opt/airflow/dags"
# defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ismail',
    'start_date': days_ago(0),
    'email': ['ismailhero@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),

}

# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Sample ETL DAG using Bash',
    schedule_interval=timedelta(minutes=1),
)

# First task: unzip the .tar file
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="tar -zxf ${DAGS_DIR}/tolldata.tgz -C ${DAGS_DIR}/ && echo ${DAGS_DIR}",
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 2nd task: Extract data from CSV
# fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file, save file: csv_data.csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 ${DAGS_DIR}/vehicle-data.csv > ${DAGS_DIR}/csv_data.csv',
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 3rd task: Extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -d$'\t' -f5,6,7 ${DAGS_DIR}/tollplaza-data.tsv | tr '\t' ',' > ${DAGS_DIR}/tsv_data.csv",
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 4th task: Extract data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-67 ${DAGS_DIR}/payment-data.txt | tr " " "," > ${DAGS_DIR}/fixed_width_data.csv',
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 4.5th task: Convert window's line-breaks in tsv_data.csv to Linux's line-breaks
# Use of raw string to avoid convertion or \r to \n
CRLF_to_LF = BashOperator(
    task_id = 'CRLF_to_LF',
    bash_command=r"cat ${DAGS_DIR}/tsv_data.csv | tr -d '\r' > ${DAGS_DIR}/tsv_data_lf.csv ",
        # && mv ${DAGS_DIR}/tsv_data_lf.csv ${DAGS_DIR}/tsv_data.csv",
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 5th task: Consolidate Data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' ${DAGS_DIR}/csv_data.csv ${DAGS_DIR}/tsv_data_lf.csv ${DAGS_DIR}/fixed_width_data.csv > ${DAGS_DIR}/extracted_data.csv",
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

# 6th task: Transform the data. Transform the vehicle_type (4th) field in extracted_data.csv into Capital letters and save it a file named transformed_data.csv
# in the staging directory.
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = "paste -d ','  <(cut -d ',' -f 1-3 ${DAGS_DIR}/extracted_data.csv) <(cut -d ',' -f 4 ${DAGS_DIR}/extracted_data.csv | tr 'a-z' 'A-Z') \
        <(cut -d ',' -f 5-9 ${DAGS_DIR}/extracted_data.csv) > ${DAGS_DIR}/staging/transformed_data.csv",
    env={'DAGS_DIR': dags_dir},  
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> CRLF_to_LF >> consolidate_data
# unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data



