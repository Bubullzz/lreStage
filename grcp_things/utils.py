import chardet
import sys
import tqdm
import grpc
import math
import datetime
import hashlib
import swhgraph_pb2
import requests
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2

def build_traversal_request(src, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD, max_depth=None, max_matching_nodes=None):
    return swhgraph_pb2.TraversalRequest(
            src=src,
            direction=direction,
            edges=edges,
            return_nodes=swhgraph_pb2.NodeFilter(types=types),
            max_depth=max_depth,
            max_matching_nodes=max_matching_nodes
        )

def my_traverse_it(stub, src, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD, max_depth=None, max_matching_nodes=None):
    req = build_traversal_request(src,edges,types, direction, max_depth, max_matching_nodes)
    return stub.Traverse(req)

# Calling SWH Traversal function, but doing it in a more cute way :)
def my_traverse(stub, src, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD, max_depth=None, max_matching_nodes=None):
    return list(my_traverse_it(stub, src, edges, types, direction, max_depth, max_matching_nodes))

def get_node(stub, swhid):
    req = swhgraph_pb2.GetNodeRequest(
            swhid=swhid
        )
    return stub.GetNode(req)

def pretty_print_date(date):
    print(datetime.datetime.fromtimestamp(date).strftime('%Y-%m-%d %H:%M:%S'))

def get_word(stub, all_origins, word):
    response_iterator = my_traverse_it(stub,all_origins,types="rev")
    for node in response_iterator:
        try:
            if word in node.rev.message.decode('utf-8'):
                print(node.rev.message.decode('utf-8').replace(word, f'\033[92m{word}\033[0m'))
                print('---')
        except UnicodeDecodeError:
            print("error decoding: ", end='')
            print(node.rev.message)


def snapshots(stub, all_origins, only_ids=False, max_depth=1):
    get_all_snapshots = swhgraph_pb2.TraversalRequest(
        src=all_origins,
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="ori:*",
        return_nodes=swhgraph_pb2.NodeFilter(types="snp"),
        max_depth=max_depth
    )
    all_snp = stub.Traverse(get_all_snapshots)
    return [snp.swhid for snp in all_snp] if only_ids else list(all_snp)

def get_parent_repo(url):
    owner = url.split("/")[-2]
    repo = url.split("/")[-1]
    url = f"https://api.github.com/repos/{owner}/{repo}"

    headers = {}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        repo_data = response.json()
        if repo_data.get("fork") and "parent" in repo_data:
            return repo_data["parent"]["html_url"]
        else:
            return "Not a Fork"
    else:
        return None
    


