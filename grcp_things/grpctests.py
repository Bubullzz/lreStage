import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import chardet
import sys
import tqdm
from utils import *
from property_checkers import *

def run():
    channel = grpc.insecure_channel('localhost:50091')

    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)

    with open(sys.argv[1], 'r') as infile:
        all_origins = []
        for line in infile:
            all_origins.append(line[:-1])

    chek_if_traverse_gives_duplicates(stub, all_origins, True)
    #how_many_snp_per_origin(stub, all_origins)
    #time_between_snapshots(stub, all_origins)

if __name__ == '__main__':
    run()
