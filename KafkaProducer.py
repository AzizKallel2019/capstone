from kafka import KafkaProducer
import csv

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'scrap'

# CSV file path
csv_file = 'outputa.csv'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic, msg.partition))

def read_csv(csv_file):
    """ Read data from CSV file. """
    with open(csv_file, 'r', newline='') as file:
        reader = csv.reader(file)
        for row in reader:
            yield ','.join(row)

def produce_messages(producer, topic, csv_file):
    """ Produce messages to Kafka topic from CSV data. """
    for message in read_csv(csv_file):
        producer.send(topic, value=message.encode('utf-8')).add_callback(delivery_report)
        producer.flush()

def main():
    # Kafka producer configuration
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    try:
        # Produce messages to Kafka topic
        produce_messages(producer, topic, csv_file)
        
    except KeyboardInterrupt:
        pass
    
    finally:
        # Close the producer
        producer.close()

if __name__ == '__main__':
    main()
