import chardet
import sys
import tqdm
import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2

def build_traversal_request(src, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD):
    return swhgraph_pb2.TraversalRequest(
            src=src,
            direction=direction,
            edges=edges,
            return_nodes=swhgraph_pb2.NodeFilter(types=types),
        )

# Calling SWH Traversal function, but doing it in a more cute way :)
def my_traverse(stub, src, edges="*", types="*", direction=swhgraph_pb2.GraphDirection.FORWARD):
    req = build_traversal_request(src,edges,types, direction)
    return list(stub.Traverse(req))

def get_node(stub, swhid):
    req = swhgraph_pb2.GetNodeRequest(
            swhid=swhid
        )
    return stub.GetNode(req)

def get_migrations(stub, all_origins):
    down_stream_request = swhgraph_pb2.TraversalRequest(
        src=all_origins,
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="ori:*,snp:*",
        return_nodes=swhgraph_pb2.NodeFilter(types="rev"),
    )
    down_stream_iterator = stub.Traverse(down_stream_request)
    for revision in down_stream_iterator:
        up_stream_request = swhgraph_pb2.TraversalRequest(
            src=[revision.swhid],
            direction=swhgraph_pb2.GraphDirection.BACKWARD,
            edges="rev:rev,rev:snp,snp:ori",
            return_nodes=swhgraph_pb2.NodeFilter(types="ori"),
        )
        up_stream_iterator = stub.Traverse(up_stream_request)
        multi_origins = []
        for origin in up_stream_iterator:
            multi_origins.append(origin)
        if len(multi_origins) > 1:
            print("found migration ??")
            for ori in multi_origins:
                print(ori.ori.url)

def get_word(stub, all_origins, word):
    request = swhgraph_pb2.TraversalRequest(
        src=all_origins,
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="*",
        return_nodes=swhgraph_pb2.NodeFilter(types="rev"),
    )
    response_iterator = stub.Traverse(request)

    i = 0
    for node in tqdm.tqdm(response_iterator):
        try:
            """
            i += 1
            if i % 10000 == 0:
                print(i)
            """
            if word in node.rev.message.decode('utf-8'):
                print(node.rev.message.decode('utf-8').replace(word, f'\033[92m{word}\033[0m'))
                print('---')

        except UnicodeDecodeError:
            print("error decoding: ", end='')
            print(node.rev.message)

# PAS FINIT
def same_commit_diff_snp_diff_url(stub, all_origins):
    get_all_first_commits_request = swhgraph_pb2.TraversalRequest(
        src=all_origins,
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="ori:*,snp:*",
        return_nodes=swhgraph_pb2.NodeFilter(types="rev"),
    )
    all_first_commits = stub.Traverse(get_all_first_commits_request)
    for commit in all_first_commits:
        snapshots_requests = swhgraph_pb2.TraversalRequest(
            src=[commit.swhid],
            direction=swhgraph_pb2.GraphDirection.BACKWARD,
            edges="rev:rev,rev:snp", #voir si des "snp:snp" existent
            return_nodes=swhgraph_pb2.NodeFilter(types="snp"),
        )
        snapshots = stub.Traverse(snapshots_requests)
        unique_ori = {}
        for snap in snapshots:
            if snap.num_successors == 1:
                origin_requests = swhgraph_pb2.TraversalRequest(
                    src=[snap.swhid],
                    direction=swhgraph_pb2.GraphDirection.BACKWARD,
                    edges="snp:ori",
                    return_nodes=swhgraph_pb2.NodeFilter(types="ori"),
                )
                for ori in stub.Traverse(origin_requests):
                    unique_ori.append(ori.swhid)
        if (len(unique_ori) > 1):
            print("found")

def snapshots(stub, all_origins, max_depth=1):
    get_all_snapshots = swhgraph_pb2.TraversalRequest(
        src=all_origins,
        direction=swhgraph_pb2.GraphDirection.FORWARD,
        edges="ori:*",
        return_nodes=swhgraph_pb2.NodeFilter(types="snp"),
        max_depth=max_depth
    )
    all_snp = stub.Traverse(get_all_snapshots)
    return list(all_snp)
