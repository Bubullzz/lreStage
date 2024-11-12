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
        for line in infile:
            all_origins.append(line[:-1])

    dataset_maker(stub, all_origins[9:])
    



if __name__ == '__main__':
    run()
    exit(0)
    db_dict = {}
    directory_path = '/masto/2024-05-16/ORC'
    for root, dirs, files in os.walk(directory_path):
        for file in tqdm.tqdm(files, desc="visiting files"):
            file_path = os.path.join(root, file)
            with open(file_path, 'rb') as orc_file:
                table = orc.ORCFile(orc_file).read()
                df = table.to_pandas()
                print(df)
                print(df.columns)
                tmpDict = dict(zip(list(df['sha1_git']),list(df['sha1'])))
                db_dict.update(tmpDict)
    exit(0)
    run()
