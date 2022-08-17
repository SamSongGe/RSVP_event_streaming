# https://github.com/joke2k/faker
# https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html
# pip install Faker
# pip install confluent-kafka

from faker import Faker
from random import seed, randint, choice
import json
import time
from kafka import KafkaProducer
import socket


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        #print("Message produced: %s" % (str(msg)))
        print("Message produced: %s" % msg)


producer = KafkaProducer(bootstrap_servers='ip-172-31-91-232.ec2.internal:9092')
topic = 'rsvp_samsongge'

state_list = ['VA', 'MD', 'CA', 'DC', 'DE']

rsvp_id_upper = 9223372036854775808
# send out one message every second
message_interval = 1

fake = Faker()
seed(1)


while True:
    dict = {}
    dict['rsvp_id'] = randint(0, rsvp_id_upper)
    dict['name'] = fake.name()
    dict['company'] = fake.company()
    dict['city'] = fake.city()
    dict['state'] = choice(state_list)

    json_string = json.dumps(dict)
    producer.send(topic, key=None, value=json_string)
    time.sleep(message_interval)
