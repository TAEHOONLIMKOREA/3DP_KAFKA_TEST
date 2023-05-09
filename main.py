# importing the required libraries
from time import sleep
from json import dumps
import json
from kafka import KafkaConsumer
from helper import kafka_helper
from helper import grpc_helper
from datetime import datetime

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

    grpc_client = grpc_helper.GrpcClient()



    my_kafka_client = kafka_helper.KafkaClient()
    # Send messages
    datetime_now = datetime.now()
    datetime_str = datetime_now.strftime("%Y%m%d_%H%M%S")
    msg = {"Time": datetime_str,
           "BuildName": self.KDIP.BuildName,
           "BuildState": "start"}
    my_kafka_client.InsertMessage('msgtest', msg)

    for i in range(10):
        datetime_now = datetime.now()
        datetime_str = datetime_now.strftime("%Y%m%d_%H%M%S")
        msg = {"Time": datetime_str,
               "Param": "TEST",
               "Value": val,

               "LayerIdx": self.KDIP.CurrentLayer,
               "tag": "Environment"}
        self.KDIP.KafkaClient.InsertMessage("msgtest", msg)



    # Kafka broker에 연결합니다.
    consumer = KafkaConsumer(
        'msgtest',  # topic 이름
        bootstrap_servers=['keties.iptime.org:55592'],
        auto_offset_reset='earliest',  # 가장 처음부터 메시지를 받습니다.
        enable_auto_commit=True,  # 자동으로 offset을 commit합니다.
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received message: {message.value}")

    print("End!")
    # Close the Kafka producer connection
    my_producer.close()