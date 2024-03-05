from kafka import KafkaConsumer
import pandas as pd
import io

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'scrap'
csv_file = 'output.csv'

def consume_messages(consumer, csv_file):
    """ Consume messages from Kafka topic and write them to a CSV file. """
    with open(csv_file, 'a') as file:  # Open file in append mode
        for message in consumer:
            try:
                # Decode message value from bytes to string
                csv_str = message.value.decode('utf-8')
                # Load CSV string as DataFrame
                df = pd.read_csv(io.StringIO(csv_str))
                # Write DataFrame to CSV file
                df.to_csv(file, index=False, header=not file.tell())  # Write header only if file is empty
                
                # Commit offsets
                consumer.commit()
            except Exception as e:
                print(f"Error processing message: {e}")

def main():
    # Kafka consumer configuration
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

    try:
        # Consume messages from Kafka topic and write them to CSV file
        consume_messages(consumer, csv_file)
        
    except KeyboardInterrupt:
        pass
    
    finally:
        # Close the consumer
        consumer.close()

if __name__ == '__main__':
    main()

