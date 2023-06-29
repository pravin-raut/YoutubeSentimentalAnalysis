from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer,KafkaConsumer

class KafkaConnect:
  num_partitions=None
  replication_factor=None

  def __init__(self,server,topic):
    self.server=server
    self.topic=topic

  def create_topic(self,num_partitions,replication_factor):
    try:
      admin_client = KafkaAdminClient(bootstrap_servers=self.server)
      new_topic = NewTopic(name=self.topic, num_partitions=num_partitions, replication_factor=replication_factor)
      # Create the topic using the Kafka Admin Client
      admin_client.create_topics(new_topics=[new_topic], validate_only=False)
      print("Kafka topic {} successfully created".format(self.topic) )
    except TopicAlreadyExistsError:
      print("Kafka topic {} already exist".format(self.topic) )

  def get_topic_list(self):
    # Initialize the Kafka Admin Client with the Kafka broker address
    admin_client = KafkaAdminClient(bootstrap_servers=self.server)

    # Get a list of all topics in the Kafka cluster
    topic_metadata = admin_client.list_topics()

    # Extract the names of all topics from the topic metadata
    topic_names = [topic for topic in topic_metadata]

    # Print the list of topic names to the console
    print("List of Kafka topics:")
    for topic_name in topic_names:
        print(topic_name)

  def deleteTopic(self,topic_name):
    try:
      # Initialize the Kafka Admin Client with the Kafka broker address
      admin_client = KafkaAdminClient(bootstrap_servers=self.server)
      # Delete the topic
      admin_client.delete_topics([topic_name])
      print("Kafka topic {} successfully deleted".format(topic_name) )
    except TopicAuthorizationFailedError:
      print("Authorization failed while attempting to delete the topic.")
    except UnknownTopicOrPartitionError:
        print(f"Topic '{topic_name}' not found.")

  def kproducer(self):
        return KafkaProducer(bootstrap_servers=self.server)



  def produceMessage(self,message_key,message_payload):
    # Initialize the Kafka Producer with the Kafka broker address
    producer = self.kproducer()
    # Publish the message with key to the Kafka topic
    producer.send(topic=self.topic, key=message_key, value=message_payload)
    # Flush the Kafka producer to ensure the message is sent
    producer.flush()
    print("Message published successfully to Kafka!")

  def kConsumer(self,group_id,offset_reset):
    return KafkaConsumer(
    self.topic,
    bootstrap_servers=self.server,
    auto_offset_reset=offset_reset,
    group_id=group_id

    )

  def consumeMessage(self,group_id,offset_reset):
    consumer = self.kConsumer(group_id,offset_reset)

    # Consume messages from the topic
    for message in consumer:
        print(f"Received Key {message.key.decode('utf-8')} & message: {message.value.decode('utf-8')} for topic {message.topic}")


