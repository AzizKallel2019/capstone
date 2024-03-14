from confluent_kafka import Consumer, KafkaError
import pandas as pd
import io

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'Departures'
topic1 = 'Arrivals'
topic2 = 'weather'
csv_file = 'Departures.csv'
csv_file1 = 'weather.csv'
csv_file2 = 'Arrivals.csv'

def consume_messages(consumer, directory, csv_file, mode='w'):
    """
    Consume messages from Kafka topic and write them to a CSV file.

    Args:
    - consumer: Kafka consumer object
    - directory: Directory where the CSV file should be saved
    - csv_file: Name of the CSV file
    - mode: File opening mode ('a' for append, 'w' for write/overwrite)

    """
    try:
        with open(f"{directory}/{csv_file}", mode) as file:  # Open file in specified mode
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        break
                    else:
                        # Error
                        print(f"Kafka error: {msg.error()}")
                        break

                try:
                    # Decode message value from bytes to string
                    csv_str = msg.value().decode('utf-8')
                    # Write CSV string to file
                    file.write(csv_str + '\n')
                    print(f"Message written to {csv_file}")
                except Exception as e:
                    print(f"Error processing message: {e}")
    except Exception as e:
        print(f"Error opening file: {e}")

def main():
    # Kafka consumer configuration
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer1 = Consumer(conf)
    consumer2 = Consumer(conf)
    
    # Specify the directory to save the CSV files
    directory1 = './'
    directory2 = './'
    directory3 = './'
    
    # Subscribe to Kafka topics
    consumer.subscribe([topic])
    consumer1.subscribe([topic1])
    consumer2.subscribe([topic2])

    try:
        #Consume messages from Kafka topic and write them to CSV file
        consume_messages(consumer, directory1, csv_file, mode='a')
        consume_messages(consumer1, directory2, csv_file2, mode='a') 
        consume_messages(consumer2, directory3, csv_file1, mode='a') 

    except KeyboardInterrupt:
        pass
    
    finally:
        # Close the consumer
        consumer.flush()
        consumer1.flush()
        consumer2.flush()
    

if __name__ == '__main__':
    main()
