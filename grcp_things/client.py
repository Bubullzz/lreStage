class Client:
    def __init__(self, ip):
        channel = grpc.insecure_channel(ip, origins)
        self.stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)
        self.origins = origins

    def my_traverse(self, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD, max_depth=None):
        req = build_traversal_request(self.origins, edges, types, direction, max_depth)
        return list(stub.Traverse(req))
