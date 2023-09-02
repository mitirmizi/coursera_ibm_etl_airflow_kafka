"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'teemo'
PASSWORD = 'lolpass123'

print("Connecting to the database")
try:
    conn = psycopg2.connect(
        host="localhost",
        database=DATABASE,
        user=USERNAME,
        password=PASSWORD,
        port="5432"
    )
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
    
cursor = conn.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
try:
    for msg in consumer:

        # Extract information from kafka
        message = msg.value.decode("utf-8")

        # Transform the date format to suit the database schema
        (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

        dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
        timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

        # Loading data into the database table

        insert_query = sql.SQL(
            "INSERT INTO livetolldata VALUES (%s, %s, %s, %s)"
        )

        result = cursor.execute(insert_query, (timestamp, vehcile_id, vehicle_type, plaza_id))
        print(f"ET: {timestamp} - A {vehicle_type} with id: {vehcile_id} at plaza: {plaza_id} was inserted into the database")
        conn.commit()

except Exception as err:
    print(f"Error occurred in sending data to postgres: {err}")
finally:
    cursor.close()
    conn.close()
