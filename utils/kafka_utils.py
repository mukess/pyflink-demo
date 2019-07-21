from kafka import KafkaProducer
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.admin import NewTopic
import json


def send_msg(topic='test', msg=None):
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    if msg is not None:
        future = producer.send(topic, msg)
        future.get()


def get_msg(topic='test'):
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest')
    for message in consumer:
        print(message)


def list_topics():
    global_consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
    topics = global_consumer.topics()
    return topics


def create_topic(topic='test'):
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topics = list_topics()
    if topic not in topics:
        topic_obj = NewTopic(topic, 1, 1)
        admin.create_topics(new_topics=[topic_obj])


if __name__ == '__main__':
    print(list_topics())
    # msg = {'user': 'flink', 'message': 'Hello Message', 'time': '2013-01-01T00:14:13Z'}
    # msg = {'user': 'flink', 'message': 'Hello Message', 'time': '1990-10-14T12:12:43Z'}
    # send_msg('test', msg)
    # get_msg('user')
