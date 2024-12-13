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

    depths = [1,3]
    depths_dir_combinations = [(depth, dir) for depth in depths for dir in [False, True]]

    url_to_run_to_migrations = {url:
                                    {depth_dir_tuple : None}
                                    for url in [url for swhid, url in l]
                                    for depth_dir_tuple in depths_dir_combinations
                                } 

    for depth in depths:
        for dir in [False, True]:
            for swhid, url in tqdm.tqdm(l):
                oris = find_migration(stub, swhid, url, depth, dir, no_snp, no_rev, no_dir)
                url_to_run_to_migrations[url][(depth, dir)] = oris




    urls_1_rev = [url for url in url_to_run_to_migrations.keys() if len(url_to_run_to_migrations[url][(1, False)]) > 1]
    urls_3_rev = [url for url in url_to_run_to_migrations.keys() if len(url_to_run_to_migrations[url][(3, False)]) > 1]
    urls_1_dir = [url for url in url_to_run_to_migrations.keys() if len(url_to_run_to_migrations[url][(1, True)]) > 1]
    urls_3_dir = [url for url in url_to_run_to_migrations.keys() if len(url_to_run_to_migrations[url][(3, True)]) > 1]

    for x in [urls_1_rev, urls_3_rev, urls_1_dir, urls_3_dir]:
        print(len(x))
    
    exc_3_rev = [url for url in urls_3_rev if url not in urls_1_rev]
    exc_1_dir = [url for url in urls_1_dir if url not in urls_1_rev]
    exc_3_dir = [url for url in urls_3_dir if url not in exc_3_rev + exc_1_dir + urls_1_rev]



    for x in [exc_3_rev, exc_1_dir, exc_3_dir]:
        print(len(x))

    for x in exc_3_rev:
        print(x)

    print('-----')
    for x in exc_1_dir:
        print(x)




if __name__ == '__main__':
    run()



