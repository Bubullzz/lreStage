import chardet
import sys
import tqdm
import grpc
import math
import datetime
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
from utils import *

# Function to check how the breadth-first search manages duplicates (does a swhid appears multiple times if multiple orgins point to it ?)
# Results : NO DUPLICATES. Even if there were multiple origins pointing to one snapshot, when traversing with all these origins the snp appeared only once.
def chek_if_traverse_gives_duplicates(stub, all_origins, print_urls=False):
    all_snp = snapshots(stub, all_origins, False,  1000)
    snp_ids = [snp.swhid for snp in all_snp]
    for id in snp_ids:
        # trying to find one snp that has multiple origins
        origins = my_traverse(stub, [id], "snp:ori", "ori", swhgraph_pb2.GraphDirection.BACKWARD)
        origins_id = set([o.swhid for o in origins])
        if len(origins_id) > 1:
            # We found a snp that has more than on origin
            if print_urls:
                for url in [o.ori.url for o in origins]:
                    print(url)
            print(f"The snp with id : {id}; has {len(origins_id)} origins.")
            print("Sending a Traversal Request will all these origins to see if current snp appears multiple times")
            final_id = id

            break
    snp_from_origins = my_traverse(stub, origins_id, "ori:snp", "snp")
    curr_snp_occ = [snp.swhid for snp in snp_from_origins if snp.swhid == final_id]
    print(f"In the traversal response with the specific origins, we got {len(curr_snp_occ)} results with id : {id}")
    if len(curr_snp_occ) == 1 :
        print("Even with multiple origins pointing to the same snapshot, it appears only once in traversal response")
    elif len(curr_snp_occ) > 1 :
        print("The snapshot id appeared in dupplicates")
    else : # len == 0 ???
        print("Sould not be printed, recheck your logic")

# Function to find how many snapshots are owned by each origin
# max : max number of children before we stop counting precisely
# if max = 5, anything with more than 5 children will be counted as 5+
# For the python dataset : only one snp per origin
def how_many_snp_per_origin(stub, all_origins, max=300):
    num_of_unique_snp_per_origin = [0 for i in range(max)] # This one is only UNIQUE snp from each origin
    for id in tqdm.tqdm(all_origins):
        children_snp = my_traverse(stub, [id], "ori:snp", "snp")
        num_of_unique_snp_per_origin[min(max - 1, len(children_snp))] += 1
    for i in range(0,max):
        if num_of_unique_snp_per_origin[i] > 0:
            print(f"there are {num_of_unique_snp_per_origin[i]} origins that have {i} snapshot children")
    print(num_of_unique_snp_per_origin[:10])
    tot = 0
    for i in range(max):
        tot += i * num_of_unique_snp_per_origin[i]
    print(str(tot/len(all_origins)) + " average children per origin")

def how_many_ori_per_snp(stub, all_origins, max=1000):
    counters = [0 for i in range(max)]
    all_snp = snapshots(stub, all_origins)
    for node in tqdm.tqdm(all_snp):
        id = node.swhid
        children_snp = my_traverse(stub, [id], "snp:ori", "ori", swhgraph_pb2.GraphDirection.BACKWARD)
        if len(children_snp) >= max :
            counters[max - 1] += 1
        else:
            counters[len(children_snp)] += 1
    for i in range(0,max):
        if counters[i] > 0:
            print(f"there are {counters[i]} snapshot that have {i} parents")

# Not working on the teaser because there is no edge data :(
def time_between_snapshots(stub, all_origins):
    for id in tqdm.tqdm(all_origins):
        node = get_node(stub, id)
        if node.num_successors <= 1:
            continue
        min = math.inf
        max = -math.inf
        for succ in node.successor:
            time = succ.label#.visit_timestamp
            if succ.label == []:
                #print("empty label, skipping")
                continue
            print(time)
            if time < min :
                min = time
            if time > max:
                max = time
        if min == math.inf or max == -math.inf or min == max:
            continue
        dt_object = datetime.datetime.utcfromtimestamp(max - min)
        formatted_date = dt_object.strftime('%Y-%m-%d %H:%M:%S')
        print(formatted_date)


# I have no idea why this snapshot has 693 parents and I want to be able to redo the experiment
# ANSWER : It is the 'empty' snapshot
# On the python teaser dataset
def weird_behavior(stub):
    id = 'swh:1:snp:1a8893e6a86f444e8be8e7bda6cb34fb1735a00e' # magic value but we need to check the properties of this specific node
    req = swhgraph_pb2.TraversalRequest(
            src=[id],
            direction=swhgraph_pb2.GraphDirection.BACKWARD,
            edges="*",
            return_nodes=swhgraph_pb2.NodeFilter(types="ori"),
        )
    nodes = list(stub.Traverse(req))
    urls = [n.ori.url for n in nodes]
    print(f"The given node has {len(nodes)} origins")
    return urls

def same_commit_different_origin(stub, all_origins):
    latest_rev_per_origin = []
    for origin in tqdm.tqdm(all_origins):
        snps = my_traverse(stub,[origin], "ori:snp", "snp") # all snp from this ori
        snps = [snp.swhid for snp in snps]
        latest_time = -math.inf
        latest_rev_swhid = ""
        revs = my_traverse(stub, snps, "snp:rev", "rev") # all rev from this ori (might be faster but is ok)
        for rev in revs:
            if latest_time < rev.rev.author_date:
                latest_time = rev.rev.author_date
                latest_rev_swhid = rev.swhid
        if latest_rev_swhid != "" :
            latest_rev_per_origin.append(latest_rev_swhid)
    for rev in tqdm.tqdm(latest_rev_per_origin):
        oris = my_traverse(stub, [rev], "*", "ori", swhgraph_pb2.GraphDirection.BACKWARD) # When working on the bigger graph we might encounter snp with multi-origins, this level of precision will not be enough
        if len(oris) > 1 :
            print("+------------------------+")
            print("found multiple origins on one commit : " + rev)
            for ori in oris:
                print(ori.ori.url + " : " + ori.swhid)


