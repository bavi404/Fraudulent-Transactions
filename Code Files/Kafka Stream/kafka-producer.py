import json
from confluent_kafka import Producer
import uuid


def read_ccloud_config(config_file):
    """
    Reads the Confluent Cloud configuration file and filters out unwanted fields.

    :param config_file: Path to the configuration file
    :return: A dictionary with Kafka configuration
    """
    omitted_fields = {'schema.registry.url', 'basic.auth.credentials.source', 'basic.auth.user.info'}
    conf = {}
    try:
        with open(config_file, 'r') as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):  # Ignore comments and empty lines
                    parameter, value = line.split('=', 1)
                    if parameter not in omitted_fields:
                        conf[parameter] = value.strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")
    except ValueError as e:
        raise ValueError(f"Error parsing configuration file: {e}")
    return conf


def produce_messages(producer, topic, messages):
    """
    Produces messages to the specified Kafka topic.

    :param producer: Kafka producer instance
    :param topic: Kafka topic name
    :param messages: List of messages (dictionaries) to produce
    """
    for message in messages:
        try:
            # Generate a unique key if "key" is not present in the message
            key_value = str(message.get("key", str(uuid.uuid4())))
            producer.produce(topic, key=key_value, value=json.dumps(message))
        except Exception as e:
            print(f"Error producing message: {e}")
    producer.flush()
    print(f"Produced {len(messages)} messages to topic '{topic}'.")


def load_json_file(file_path):
    """
    Loads a JSON file and returns its content.

    :param file_path: Path to the JSON file
    :return: Parsed JSON data
    """
    try:
        with open(file_path, 'r') as json_file:
            return json.load(json_file)
    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file '{file_path}' not found.")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing JSON file: {e}")


if __name__ == "__main__":
    # File paths
    config_file = "D:/DBDA Project/Code Files/Kafka Stream/client.properties"
    json_file_path = "D:/DBDA Project/Code Files/Kafka Stream/sv_to_json_47627147.json"

    # Kafka topic
    kafka_topic = "new"

    try:
        # Load JSON messages
        messages_from_json = load_json_file(json_file_path)

        # Initialize Kafka producer
        kafka_config = read_ccloud_config(config_file)
        producer = Producer(kafka_config)

        # Produce messages to Kafka
        produce_messages(producer, kafka_topic, messages_from_json)

    except Exception as e:
        print(f"An error occurred: {e}")
