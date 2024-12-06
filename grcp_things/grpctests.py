import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import sys
import tqdm
from utils import *
from property_checkers import *
from content_reader import *
from dataset_maker import *

def run():
    channel = grpc.insecure_channel('localhost:50091')

    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)

    with open(sys.argv[1], 'r') as infile:
        all_origins = []
        i = 0
        for line in tqdm.tqdm(infile, total=295060682):
            all_origins.append(line[:-1])
            i += 1
            #if i == 1000:
                

    target = 131072
    batch_size = 2048
    start = 0
    ext = [
        'c', 'cc', 'py', 'cpp', 'java', 'js', 'ts', 'cs', 'go', 'rb', 'php', 
        'swift', 'kt', 'rs', 'sh', 'lua', 'r', 'scala', 'hs'
        ]

    for i in range(0, len(all_origins), batch_size):
        start = dataset_maker(stub, all_origins[i:i+batch_size], start_index=start, extension=ext, out_dir='./rev2rev_dataset_100K')
        print(start, '/', target)
        if start >= target:
            break

    #dataset_maker(stub, all_origins[10:10+64])
    



if __name__ == '__main__':
    run()

