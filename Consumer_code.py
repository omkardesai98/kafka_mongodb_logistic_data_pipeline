from datetime import datetime
from dotenv import load_dotenv
import os
import urllib.parse
from pymongo import MongoClient
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

# Load environment variables
load_dotenv()

# Kafka configuration
kafka_config = {
    'bootstrap.servers': os.environ.get('bootstrap.servers'),
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.environ.get('sasl.username'),
    'sasl.password': os.environ.get('sasl.password'),
    'group.id': 'group1',
    'auto.offset.reset': 'latest'
}

# Date parsing function with added ISO 8601 format
def parse_date(value):
    for fmt in ('%Y/%m/%d %H:%M:%S.%f', '%m/%d/%Y %H:%M', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S'):
        try:
            parsed_date = datetime.strptime(value, fmt)
            return parsed_date
        except ValueError:
            pass
    return None

# Data conversion function with added debugging
def convert_data(record):
    if record.get('timestamp'):
        print(f"Original timestamp: {record['timestamp']}")
        record['timestamp'] = parse_date(record['timestamp'])
        print(f"Converted timestamp: {record['timestamp']}")
    return record

# Delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for record {msg.key()}: {err}')
    else:
        print(f'Message delivered successfully to partition {msg.partition()} of topic {msg.topic()} at offset {msg.offset()}')

# Connect to MongoDB
escaped_password = urllib.parse.quote_plus(os.environ.get('Mongodb_Password'))
conn = f"mongodb+srv://omkar:{escaped_password}@mongodb-cluster.mqazenv.mongodb.net/?retryWrites=true&w=majority&appName=mongoDB-cluster"
mongo_client = MongoClient(conn)
db = mongo_client['Fedex_data']
collection = db['Fedex_logistic']

# Schema Registry configuration
schema_registry_client = SchemaRegistryClient({
    'url': os.environ.get('schema_url'),
    'basic.auth.user.info': f"{os.environ.get('schema_api_key')}:{os.environ.get('schema_api_secret_key')}"
})

subject_name = 'Fedex_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Deserializers
key_deserializer = StringDeserializer('utf-8')
value_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Kafka consumer configuration
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'sasl.mechanism': kafka_config['sasl.mechanism'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': value_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

# Subscribe to topic
consumer.subscribe(['Fedex_data'])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        key = msg.key()
        print(f'Successfully consumed: {msg.value()} for {key}')
        converted_value = convert_data(msg.value())
        collection.insert_one(converted_value)
        print("Record inserted successfully in MongoDB")

except KeyboardInterrupt:
    print('Consumer stopped due to keyboard interruption')
finally:
    consumer.close()
    mongo_client.close()