
from kafka.admin import (KafkaAdminClient, NewTopic,
                         ConfigResource, ConfigResourceType)
from kafka import KafkaProducer, KafkaConsumer
import json

if __name__ == '__main__':

    # To use kafka we need to create KafkaAdminClient object. 
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

    # Topic we want to create
    topic_name = "toll"

    # Get the list of topics
    topic_list = admin_client.list_topics()

    if topic_name in topic_list:
        print(f"Topic {topic_name} exists")
    else:
        print(f"Creating Topic {topic_name}")
        # Create a topic `toll`
        topic_list = []
        new_topic = NewTopic(name="toll", num_partitions=2, replication_factor=1)
        topic_list.append(new_topic)

        admin_client.create_topics(new_topics=topic_list)

        