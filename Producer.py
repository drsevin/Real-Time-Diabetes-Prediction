from confluent_kafka import Producer
import time
import csv
import json

conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'diabet-producer'
}

producer = Producer(conf)
topic = 'diabet'
csv_file = 'DiabetsProducer.csv'

def produce_to_kafka(row):
    try:
        json_data = json.dumps({
            'Pregnancies': int(row['Pregnancies']),
            'Glucose': int(row['Glucose']),
            'BloodPressure': int(row['BloodPressure']),
            'SkinThickness': int(row['SkinThickness']),
            'Insulin': int(row['Insulin']),
            'BMI': float(row['BMI']),
            'DiabetesPedigreeFunction': float(row['DiabetesPedigreeFunction']),
            'Age': int(row['Age']),
            'Outcome': int(row['Outcome'])
        })
        producer.produce(topic, key=row['Pregnancies'], value=json_data)
        producer.flush()

        print(f"Sent data to Kafka: {json_data}")

    except Exception as e:
        print(f"Error producing data to Kafka: {e}")

with open(csv_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        produce_to_kafka(row)
        time.sleep(2)

producer.flush()
producer.close()