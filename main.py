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

    scn_img, depo_img = grpc_client.GetVisionData(1)

    scanning_vision_msg = {"LayerNum": str(scn_img.LayerNum),
                           "ImageClass": "scanning",
                           "data": scn_img.Datas}

    deposition_vision_msg = {"LayerNum": str(depo_img.LayerNum),
                             "ImageClass": "scanning",
                             "data": depo_img.Datas}
    # Send messages
    my_kafka_client.InsertMessage('msgtest_large', depo_img.Datas)


    # # Send messages
    # datetime_now = datetime.now()
    # datetime_str = datetime_now.strftime("%Y%m%d_%H%M%S")
    # msg = {"Time": datetime_str,
    #        "BuildName": "TEST_BUILD",
    #        "BuildState": "start"}
    # my_kafka_client.InsertMessage('msgtest_large', msg)
    #
    # for i in range(10):
    #     datetime_now = datetime.now()
    #     datetime_str = datetime_now.strftime("%Y%m%d_%H%M%S")
    #     msg = {"Time": datetime_str,
    #            "Param": "TEST_PARAM",
    #            "Value": i*100,
    #
    #            "LayerIdx": i,
    #            "tag": "Environment"}
    #     my_kafka_client.InsertMessage("msgtest", msg)
    #
    # my_kafka_client.CloseProducer()
    #
    # # Kafka broker에 연결합니다.
    consumer = KafkaConsumer(
        'msgtest_large',  # topic 이름
        bootstrap_servers=['keties.iptime.org:55592'],
        auto_offset_reset='earliest',  # 가장 처음부터 메시지를 받습니다.
        enable_auto_commit=True,  # 자동으로 offset을 commit합니다.
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received message: {message.value}")

    print("End!")
    # Close the Kafka producer connection
