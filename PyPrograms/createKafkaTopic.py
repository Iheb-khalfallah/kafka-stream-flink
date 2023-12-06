from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')

topic_list = [
    NewTopic(name="test", num_partitions=1, replication_factor=1),
    NewTopic(name="urgent_data", num_partitions=1, replication_factor=1),
    NewTopic(name="normal_data", num_partitions=1, replication_factor=1)
]

for topic in topic_list:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")

