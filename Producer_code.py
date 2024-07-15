import time
from faker import Faker
from dotenv import load_dotenv
import random
import os
from pymongo import MongoClient
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

load_dotenv()

kafka_config = {
    'bootstrap.servers': os.environ.get('bootstrap.servers'),
    'sasl.mechanism':'PLAIN',
    'security.protocol':'SASL_SSL',
    'sasl.username':os.environ.get('sasl.username'),
    'sasl.password':os.environ.get('sasl.password')

}

fake = Faker()

def generate_sample_data():
    shipment_id = f"SH{random.randint(100000, 999999)}"
    origin = fake.city() + ", " + fake.state_abbr()
    destination = fake.city() + ", " + fake.state_abbr()
    status = random.choice(["in-transit", "delivered", "pending", "cancelled"])
    timestamp = fake.iso8601()
    
    sample_data = {
        "shipment_id": shipment_id,
        "origin": origin,
        "destination": destination,
        "status": status,
        "timestamp": timestamp
    }
    
    return sample_data

def delivery_report(err,msg):
    if err is not None:
        print(f'delivery falied for the record {msg.key()} : {err}')
    else:
        print(f'message delivered successfully on partition {msg.partition()} to the topic {msg.topic()} at offset {msg.offset()}')

# schema registry 
schema_registry_client = SchemaRegistryClient({'url':os.environ.get('schema_url'),'basic.auth.user.info':f'{os.environ.get('schema_api_key')}:{os.environ.get('schema_api_secret_key')}'})

subject_name = 'Fedex_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer = StringSerializer('utf-8')
value_serializer = AvroSerializer(schema_registry_client,schema_str)

producer = SerializingProducer(
    {
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanism':kafka_config['sasl.mechanism'],
    'security.protocol':kafka_config['security.protocol'],
    'sasl.username':kafka_config['sasl.username'],
    'sasl.password':kafka_config['sasl.password'],
    'key.serializer':key_serializer,
    'value.serializer':value_serializer
    }

)

try:
    for i in range(0,51):
        value = generate_sample_data()
        producer.produce('Fedex_data',key=str(i),value=value,on_delivery=delivery_report)
        producer.flush()
        # time.sleep(1)
except KeyboardInterrupt as e:
    print('Producer Stopped due to keyboard Interruption')
finally:
    producer.flush()


