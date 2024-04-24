from kafka import KafkaConsumer

def custom_consumer(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

    for message in consumer:
        transaction = json.loads(message.value.decode('utf-8'))
        # Perform custom analysis here
        print("Custom analysis:", transaction)

if __name__ == "__main__":
    topic = 'preprocessed_data_topic'
    custom_consumer(topic)
