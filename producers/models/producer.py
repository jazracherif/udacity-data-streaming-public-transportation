"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

from requests import Session
import socket
print (socket)
hostname = 'localhost'
# print(socket.gethostname())
print(socket.getaddrinfo(hostname, 8081))


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
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",
            "schema.registry.url": "http://localhost:8081"
            }

        self.client = AdminClient(
                    {
                      # 'debug': 'admin',                       
                     "bootstrap.servers": "localhost:9092"
                     }
                )

        # s = Session()
        # s.request("POST", "http://localhost:8081/subjects/com-stations-value/versions",
        #     {'Accept': 'application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json', 'Content-Length': '1', 'Content-Type': 'application/vnd.schemaregistry.v1+json'},
        #     {'schema': '{"type": "record", "name": "arrival_value", "namespace": "com_udacity", "fields": [{"type": "long", "name": "station_id"}, {"type": "long", "name": "train_id"}, {"type": "string", "name": "direction"}, {"type": "string", "name": "line"}, {"type": "string", "name": "train_status"}, {"type": ["null", "long"], "name": "prev_station_id"}, {"type": ["null", "string"], "name": "prev_direction"}]}'}
        #     )

        # exit()

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
                    config=self.broker_properties,
                    default_key_schema=self.key_schema, 
                    default_value_schema=self.value_schema                    
                )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        logger.info(f"creating topic {self.topic_name}")

        fs = self.client.create_topics(
                        [NewTopic(self.topic_name, 
                                  num_partitions=self.num_partitions,
                                  replication_factor=self.num_replicas)])

        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close")
        self.producer.close()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
