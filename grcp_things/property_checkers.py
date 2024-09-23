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
    all_snp = snapshots(stub, all_origins, 1000)
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
    print(num_of_unique_snp_per_origin)
    tot = 0
    for i in range(max):
        tot += i * num_of_unique_snp_per_origin[i]
    print(tot/len(all_origins))

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

def how_many_ori_per_snp(stub, all_origins, max=1000):
    counters = [0 for i in range(max)]
    all_snp = snapshots(stub, all_origins)
    for id in tqdm.tqdm(all_snp):
        children_snp = my_traverse(stub, [id], "snp:ori", "ori")
        if len(children_snp) >= max :
            counters[max - 1] += 1
        else:
            counters[len(children_snp)] += 1
    print(counters)