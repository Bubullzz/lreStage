import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import chardet


import os
def run():
    # Create a gRPC channel to the server
    channel = grpc.insecure_channel('localhost:50091')  # Replace with your server address

    # Create a stub (client)
    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)
    with open('./pythonCompressed/origins.txt', 'r') as infile:
        all_origins = []
        for line in infile:
            all_origins.append(line[:-1])

    # Create a TraversalRequest message
    request = swhgraph_pb2.TraversalRequest(
        src=all_origins,  # Replace with your source node SWHIDs
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="*",
        return_nodes=swhgraph_pb2.NodeFilter(types="rev"),
        #mask=field_mask_pb2.FieldMask()#paths=["rev.message"]),  # Only return the message field
    )

    # Make the gRPC call
    response_iterator = stub.Traverse(request)

    # Filter commit messages containing the word 'hello'
    i = 0
    for node in response_iterator:
        try:
            i += 1
            if i % 10000 == 0:
                print(i)
            if 'hello' in node.rev.message.decode('utf-8'):
                print(node.rev.message.decode('utf-8'))
                print('---')
        except UnicodeDecodeError:
            print("error decoding: ", end='')
            print(node.rev.message)



if __name__ == '__main__':
    run()
