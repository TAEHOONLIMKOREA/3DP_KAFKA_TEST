# importing the required libraries
from time import sleep
from json import dumps
import json
from kafka import KafkaConsumer
from helper import kafka_helper
from helper import grpc_helper
from datetime import datetime
import base64
import cv2
import numpy as np


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

    scanning_vision_msg = {"LayerNum": scn_img.LayerNum,
                           "ImageClass": "scanning",
                           "ImageData": None}

    deposition_vision_msg = {"LayerNum": depo_img.LayerNum,
                             "ImageClass": "scanning",
                             "ImageData": None}
    #
    scanning_vision_msg['ImageData'] = base64.b64encode(scn_img.Datas).decode('utf-8')
    deposition_vision_msg['ImageData'] = base64.b64encode(depo_img.Datas).decode('utf-8')

    # Send messages
    my_kafka_client.InsertMessage('msgtest_large', deposition_vision_msg)


    # Send messages
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

    my_kafka_client.CloseProducer()

    # Kafka broker에 연결합니다.
    consumer = KafkaConsumer(
        bootstrap_servers=['keties.iptime.org:55592'],
        auto_offset_reset='earliest',  # 가장 처음부터 메시지를 받습니다.
        enable_auto_commit=True,  # 자동으로 offset을 commit합니다.
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    topics = consumer.topics()

    print(topics)
    # for message in consumer:
    #     data_dict = message.value
    #     binary_data = base64.b64decode(data_dict['ImageData'])
    #     # 바이너리 데이터를 처리하는 코드 작성
    #
    #     # 바이트 데이터를 OpenCV 이미지 객체로 변환
    #     img = cv2.imdecode(np.frombuffer(binary_data, dtype=np.uint8), -1)
    #     cv2.imshow('image', img)
    #     cv2.waitKey(0)
    #     cv2.destroyAllWindows()



    print("End!")
    # Close the Kafka producer connection
