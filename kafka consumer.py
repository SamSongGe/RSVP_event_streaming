# https://kafka-python.readthedocs.io/en/master/index.html

from kafka import KafkaConsumer

consumer = KafkaConsumer('meetup_rsvp', bootstrap_servers='ip-172-31-91-232.ec2.internal:9092,ip-172-31-89-11.ec2.internal:9092')
for message in consumer:
    print (message.value)
