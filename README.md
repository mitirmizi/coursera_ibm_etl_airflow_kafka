# Coursera - IBM - ETL and Data Pipelines with Shell, Airflow and Kafka

## Final Assignment
Used Airflow Docker Image.

## ETL Lab (part 1/2)
### Instructions to setup airflow-docker
Follow this tutorial: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

### TL;DR of how to get started

#### Initialize the database 
(Required the first time only)
```bash
docker compose up airflow-init
```
The account created has the login `airflow` and the password `airflow`


#### Running Airflow
```bash
docker compose up
```

#### Stopping Airflow
```bash
docker compose down
```

### Issues
#### Task 1.3
The airflow docker-compose version 3.8, when it runs a task the current directory is a temporary directory, 
therefore, we need to use absolute paths for the src files and destination files during our ETLs. 
Also Due to airflow running in docker, it cannot access the file paths in our local machine, 
so we need to use the path names from the docker container's perspective. Airflow-docker mounts 
the local airflow directory as
`${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags` (`AIRFLOW_PROJ_DIR`=`/home/teemo/airflow` in this case) 
but if we want to access the file 
`${AIRFLOW_PROJ_DIR:-.}/dags/tolldata.tgz` within DAG `BashOperator` as simply `/home/teemo/airflow/dags/tolldata.tgz`.
We need to use the path `/opt/airflow/dags/tolldata.tgz`, which is where the files will be from the airflow container's 
perspective.

#### In task 1.7: consolidate data. 
The files contain windows line breaks which does not allow `paste` to work properly when merging the files. 
To check if a file has window's line breaks use the tool `od`. Following is an example
```bash
[stc@se] $ echo -e "foo\r" > a ; echo -e "bar\r" > b
[stc@se] $ od -c a
0000000   f   o   o  \r  \n
0000005
[stc@se] $ paste a b
foo     bar
[stc@se] $ paste -d"," a b
,bar
```

Use one of the following methods to first convert the line breaks to Linux linebreaks.

1. Install `dos2unix`, use it to convert the line breaks.

    ` dos2unix filename `

2. Use `sed`, a stream editor for filtering and transforming text. This will remove the carriage return (`\r`) characters from the file, 
effectively converting it from CRLF to LF line breaks. 

    `sed -i 's/\r//' filename`

3. Use `tr` command to remove the `\r` effectively accomplishing the same as above method.

    `cat filename | tr -d '\r' > newfilename`

## Kafka Lab (part 2/2)

### Download the toll traffic simulator
Use tool `wget` to download the file from the following link.
Link: https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/toll_traffic_generator.py


### Prepare the lab environment
I'm using postgres instead of MySQL as that was already installed on my machine and effectively they serve the same purpose.

#### Setup Kafka
Kafka install guide and quickstart.
Links: https://www.apache.org/dyn/closer.cgi?path=/kafka/3.5.0/kafka_2.13-3.5.0.tgz
and https://kafka.apache.org/quickstart

Then cd into kafka folder to avoid config path issues. Start zookeeper
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Then start kafka:
```bash
bin/kafka-server-start.sh config/server.properties
```


#### Setup Database
1. Create database named `tolldata`.
2. Create `livetolldata`
    ```SQL
    CREATE TABLE livetolldata(time_stamp timestamp, vehicle_id int, vehicle_type varchar(15), toll_plaza_id smallint);
    ```
