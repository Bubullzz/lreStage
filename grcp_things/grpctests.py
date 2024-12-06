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

    no_snp = []
    no_rev = []
    no_dir = []
    url_to_origins_1_rev = []
    for swhid, url in tqdm.tqdm(l):
        oris = find_migration(stub, swhid, 1, False, no_snp, no_rev, no_dir)
        url_to_origins_1_rev.append((url, oris))
    
    url_to_origins_3_rev = []
    for swhid, url in tqdm.tqdm(l):
        oris = find_migration(stub, swhid, 3, False, no_snp, no_rev, no_dir)
        url_to_origins_3_rev.append((url, oris))
    url_to_origins_1_dir = []
    for swhid, url in tqdm.tqdm(l):
        oris = find_migration(stub, swhid, 1, True, no_snp, no_rev, no_dir)
        url_to_origins_1_dir.append((url, oris))
    url_to_origins_3_dir = []
    for swhid, url in tqdm.tqdm(l):
        oris = find_migration(stub, swhid, 3, True, no_snp, no_rev, no_dir)
        url_to_origins_3_dir.append((url, oris))

    for url_to_origins in [url_to_origins_1_rev, url_to_origins_3_rev, url_to_origins_1_dir, url_to_origins_3_dir]:
        ori_0_parent = [(url, oris) for url, oris in url_to_origins if len(oris) == 0]
        ori_1_parent = [(url, oris) for url, oris in url_to_origins if len(oris) == 1]
        ori_2_more_parent = [(url, oris) for url, oris in url_to_origins if len(oris) >= 2]
        print("number of 0 origins :", len(ori_0_parent))
        print("number of 1 origins :", len(ori_1_parent))
        print("number of 2+ origins :", len(ori_2_more_parent))
        print("no_snp :", len(no_snp))
        print("no_rev :", len(no_rev))
        print("no_dir :", len(no_dir))

    ori_2_more_parent = sorted(ori_2_more_parent, key=lambda entry : len(entry[1]))

    for url, oris in ori_2_more_parent:
        swhid = [ori for ori, u in l if u == url][0]
        print(url, ':', swhid)
        for ori in oris:
            print(' ' * 32, ori.ori.url)
    



if __name__ == '__main__':
    run()

