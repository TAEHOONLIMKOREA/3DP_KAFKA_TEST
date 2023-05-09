# importing the required libraries
from json import dumps
from kafka import KafkaProducer

# Press the green button in the gutterf  to run the script.

class KafkaClient(object):
    def __init__(self, kdip):
        self.KDIP = kdip
        self.Producer = my_producer = KafkaProducer(
        bootstrap_servers=['keties.iptime.org:55592'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    def InsertMessage(self, topic_name, data):
        # Send messages
        self.Producer.send(topic_name, data)


    def CloseProducer(self):
        # Close the Kafka producer connection
        self.Producer.close()