import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import sys
from tqdm import tqdm
from utils import my_traverse
from migrations_finder import find_migration
from typing import List
from forks_statter import plot_fork_stats


def run():
    # Connect to the gRPC server
    channel = grpc.insecure_channel('localhost:50091')
    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)

    # Read the list of Inria sources in format 'swhid: url' (inria_pairs.txt)
    sources : List[(str, str)]= []
    with open(sys.argv[1], 'r') as infile:
        for line in infile:
            sources.append((line.split(': ')[0],line.split(': ')[1][:-1]))

    # Parameters for the search
    depth = 1
    dir = True
    print_results = False
    check_github_fork = True

    migrations : List[(str, List[(str, int)])] = [] # All the (url,fork_status) list associated to each given source
    for swhid, url in tqdm(sources, desc='Searching for migrations'):
        current_url_migrations = find_migration(stub, swhid, url, depth, dir, check_github_fork=check_github_fork)
        migrations.append((url, current_url_migrations))
        
    # Filter out the urls that have no migrations then sort by the number of migrations
    migrations = [(url,migr) for (url,migr) in migrations if len(migr) > 1]
    migrations.sort(key=lambda x: len(x[1]), reverse=False)

    # Print the results in the console
    if print_results:
        for url, migrations in migrations:
            print(url + ':')
            for url, fork_status in migrations:
                print(f'{" " * 30} {fork_status} : {url}')

    # Plot the results
    all_fork_status = [
        [fork_status for url, fork_status in associated_migrations]
        for source, associated_migrations in migrations]
    
    plot_fork_stats(all_fork_status, output_path="plot.png")

if __name__ == '__main__':
    run()



