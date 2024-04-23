from kafka import KafkaConsumer

def consumer_application(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

    for message in consumer:
        print("Consumer 3:", message.value.decode('utf-8'))

if __name__ == "__main__":
    topic = 'preprocessed_data_topic'
    consumer_application(topic)
