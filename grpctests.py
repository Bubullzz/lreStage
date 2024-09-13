import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import chardet


def run():
    channel = grpc.insecure_channel('localhost:50091')

    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)

    """
    def request_children(swhid: str):
        return swhid_to_node(swhid).successor

    def swhid_to_node(swhid: str):
        requestNode = swhgraph_pb2.GetNodeRequest(
            swhid=swhid
        )
        responseNode = stub.GetNode(requestNode)
        return responseNode
    """

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
    """
    child1 = request_children(all_origins[1])[0].swhid
    print("child 1:   " + child1)
    child2 = request_children(child1)[0].swhid
    print("child 2:   " + child2)
    print(swhid_to_node(child2).rev)
    child3 = request_children(child2)[0].swhid
    print("child 3:   " + child3)
    print(request_children(child1))
    print("________________________")
    print(request_children(child2))
    print("________________________")
    #print(request_children(child3))
    print("________________________")
    return
    """
    response_iterator = stub.Traverse(request)

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
