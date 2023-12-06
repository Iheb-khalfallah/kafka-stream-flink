from kafka import KafkaProducer
from pymongo import MongoClient
import json
import datetime
from bson import ObjectId  # Import ObjectId from bson module
from time import sleep

def data_to_kafka(nb_patient=10, n=2):
    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))  

    # Connect to MongoDB
    client = MongoClient('mongodb://root:root@localhost:27017/?authMechanism=DEFAULT')
    medical_col = client['Patient']['medical_data']

    # Fetch data from MongoDB
    data = medical_col.find()

    for record in data:
        # Extract relevant information from the MongoDB record
        patient_id = str(record.get('_id')) 
        type_record = record.get('stroke')  

        # The kafka Topic is specified based on the type_record value (1 or 0)
        topic_name = "urgent_data" if type_record == 1 else "normal_data"
        print("this is type_record",type_record)

        # Prepare the data to be sent to Kafka
        doc = {
            'date': str(datetime.datetime.now().date()),
            'time': str(datetime.datetime.now().time())[:5],
            'Patient_ID': patient_id,
            'data': {key: value for key, value in record.items() if key not in ['sex', 'age', '_id', 'stroke']}
        }

        # Send data to Kafka
        producer.send(topic_name, doc)
        print("This data is urgent!!!" if type_record == 1 else "This data is normal")
        print(doc)
        sleep(n)

    producer.flush()

# Call the function with nb_patient=6
data_to_kafka(nb_patient=50,n=2)

