import random
import time
from kafka import KafkaProducer
from kafka import errors
from json import dumps
from time import sleep

def write_data(producer):
    data_cnt = 20000
    order_id = calendar.timegm(time.gmtime())
    topic = "patient_data"  # Adjust the topic name to match your Kafka topic

    for i in range(data_cnt):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        rd = random.random()
        order_id += 1

        patient_data = {
            'date': '2023-12-12',
            'time': time.strftime("%H:%M", time.localtime()),
            'Patient_ID': f'patient_{order_id}',
            'data': {
                'hypertension': random.choice([0.0, 1.0]),
                'heart_disease': random.choice([0.0, 1.0]),
                'ever_married': random.choice([0.0, 1.0]),
                'work_type': random.choice([0.0, 1.0, 2.0, 3.0, 4.0]),  # Adjust the range based on your actual values
                'Residence_type': random.choice([0.0, 1.0]),
                'avg_glucose_level': round(random.uniform(80, 200), 2),
                'bmi': round(random.uniform(18.5, 35), 1),
                'smoking_status': random.choice([0.0, 1.0]),
            }
        }

        producer.send(topic, value=patient_data)
        sleep(0.5)

def create_producer():
    print("Connecting to Kafka brokers")
    for i in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))

            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    producer = create_producer()
    write_data(producer)

