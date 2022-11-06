"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # : Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "client.id": "sim-producer",
            "bootstrap.servers": "PLAINTEXT://localhost:9092"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # : Configure the AvroProducer
        avro_config = self.broker_properties
        avro_config.update({"schema.registry.url": "http://localhost:8081"})
        self.producer = AvroProducer(
            config=avro_config,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # : Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        admin = AdminClient(self.broker_properties)
        topic_info = admin.list_topics(topic=self.topic_name)
        if not topic_info.topics:
            logger.debug(f"creating kafka topic {self.topic_name}")
            admin.create_topics([NewTopic(self.topic_name, self.num_partitions, self.num_replicas)])
        else:
            logger.debug(f"kafka topic {self.topic_name} already exists")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # : Write cleanup code for the Producer here
        #
        #
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
