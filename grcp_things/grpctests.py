import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import sys
import tqdm
from utils import *
from property_checkers import *
from content_reader import *

def run():
    channel = grpc.insecure_channel('localhost:50091')

    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)

    with open(sys.argv[1], 'r') as infile:
        all_origins = []
        for line in infile:
            all_origins.append(line[:-1])

    dir_id = ["swh:1:dir:870c98922867e3a35874e2caf0a5d1766002a4e2"]

    revs = my_traverse(stub, dir_id, "*", "cnt", direction=swhgraph_pb2.GraphDirection.FORWARD, max_depth=None)

    ids = [r.swhid for r in revs]
    directory_path = '/work/PythonOrc/content/'
    sha1 = sha1_git_to_sha1(directory_path, ids)
    print(sha1)
    return
    for s in sha1:
        print("+-------------------------------------------------------------------------------------------+")
        print(get_file_content(s))


if __name__ == '__main__':
    run()
