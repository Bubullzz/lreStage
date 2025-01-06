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


# Calls the GitHub API to check if a given URL is a fork or not
def is_fork(url):
    owner = url.split("/")[-2]
    repo = url.split("/")[-1]
    url = f"https://api.github.com/repos/{owner}/{repo}"
    token = '' # XXX : Add your GitHub token here
    if token == '':
        raise Exception("You need to provide a GitHub token")
    headers = {"Authorization": f"token {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        repo_data = response.json()
        if repo_data.get("fork") and "parent" in repo_data:
            return 0 # Explicit Fork
        else:
            return 1 # Explicit Not a Fork
    else:
        return 2 # Don't know
