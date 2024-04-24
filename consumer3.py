from kafka import KafkaConsumer
from pymongo import MongoClient

def consumer_application(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

    # Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['myDB']  
    collection = db['collection3']  

    for message in consumer:
        message_value = message.value.decode('utf-8')
        print("Consumer 3:", message_value)
        
        # Insert message into MongoDB
        collection.insert_one({'message': message_value})
        print("Message saved to MongoDB")

if __name__ == "__main__":
    topic = 'preprocessed_data_topic'
    consumer_application(topic)
