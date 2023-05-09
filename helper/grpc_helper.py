import grpc

import KDMPgRPC_pb2
import KDMPgRPC_pb2_grpc


class GrpcClient(object):

    def __init__(self):
        self.host = 'localhost'
        self.server_port = 50052
        
        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = KDMPgRPC_pb2_grpc.KDIP_NetServiceStub(self.channel)
        message = KDMPgRPC_pb2.Message()
        message.UserID = "server"
        message.UserMessage = "Success Connection"
        response = self.stub.ClientSignalService(message)
        print(response.UserMessage)

    def GetVisionData(self, layer_num):
        """
        Client function to call the rpc for GetServerResponse
        """
        scanning_message = KDMPgRPC_pb2.Message()
        scanning_message.UserID = "server"
        scanning_message.UserMessage = str(layer_num) + " Layer_FirstShot"

        depositioning_message = KDMPgRPC_pb2.Message()
        depositioning_message.UserID = "server"
        depositioning_message.UserMessage = str(layer_num) + " Layer_SecondShot"

        return self.stub.VisionDataService(scanning_message), self.stub.VisionDataService(depositioning_message)



