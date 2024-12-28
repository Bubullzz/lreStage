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

    l = []
    with open(sys.argv[1], 'r') as infile:
        for line in infile:
            l.append((line.split(': ')[0],line.split(': ')[1][:-1]))

    depth = 1
    dir = True
    res = []
    for swhid, url in tqdm.tqdm(l):
        migrations = find_migration(stub, swhid, url, depth, dir, check_github_fork=True)
        res.append((url, migrations))
    res = [(url,migr) for (url,migr) in res if len(migr) > 1]
    res.sort(key=lambda x: len(x[1]), reverse=False)

    for url, migrations in res:
        print(url + ':')
        for url, fork_status in migrations:
            print(f'{" " * 30} {fork_status} : {url}')

if __name__ == '__main__':
    run()



