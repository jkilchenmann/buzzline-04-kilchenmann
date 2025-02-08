import json
import os
import pathlib
import time
import matplotlib.pyplot as plt
from collections import Counter
from dotenv import load_dotenv

# Import Kafka only if available
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Import logging utility
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for Environment Variables
#####################################

def get_kafka_topic() -> str:
    return os.getenv("PROJECT_TOPIC", "buzzline-topic")

def get_kafka_server() -> str:
    return os.getenv("KAFKA_SERVER", "localhost:9092")

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

#####################################
# Define Message Consumer
#####################################

def consume_from_kafka():
    """
    Consume messages from a Kafka topic.
    """
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()
    message_lengths = []
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        logger.info(f"Kafka consumer connected to {kafka_server}, listening to topic '{topic}'")
        
        for message in consumer:
            logger.info(f"Received from Kafka: {message.value}")
            process_message(message.value, message_lengths)
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
    finally:
        plot_message_length_distribution(message_lengths)


def consume_from_file():
    """
    Consume messages from the JSON file if Kafka is unavailable.
    """
    if not DATA_FILE.exists():
        logger.error("Data file not found.")
        return
    
    logger.info(f"Reading messages from {DATA_FILE}")
    message_lengths = []
    try:
        with DATA_FILE.open("r") as f:
            for line in f:
                message = json.loads(line.strip())
                logger.info(f"Read from file: {message}")
                process_message(message, message_lengths)
                time.sleep(1)  # Simulate processing delay
    except Exception as e:
        logger.error(f"Error reading from file: {e}")
    finally:
        plot_message_length_distribution(message_lengths)


def process_message(message: dict, message_lengths: list):
    """
    Process the incoming message and track message lengths.
    """
    logger.info(f"Processing message: {message}")
    message_length = message.get("message_length", 0)
    message_lengths.append(message_length)


def plot_message_length_distribution(message_lengths):
    """
    Generate a bar chart for message length frequency.
    """
    if not message_lengths:
        logger.info("No message lengths to plot.")
        return
    
    length_counts = Counter(message_lengths)
    lengths, counts = zip(*sorted(length_counts.items()))
    
    plt.figure(figsize=(10, 5))
    plt.bar(lengths, counts, color='blue')
    plt.xlabel("Message Length")
    plt.ylabel("Frequency")
    plt.title("Distribution of Message Lengths")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show(block=True)
    time.sleep(5)  # Keep the plot open for a few seconds


def main():
    logger.info("START consumer...")
    if KAFKA_AVAILABLE:
        consume_from_kafka()
    else:
        logger.warning("Kafka not available, falling back to file consumption.")
        consume_from_file()
    
#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
