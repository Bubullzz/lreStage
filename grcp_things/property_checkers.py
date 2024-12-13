import chardet
import sys
import tqdm
import grpc
import math
import datetime
import heapq
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import networkx as nx

from utils import *

BAD_SWHID = [
    'swh:1:dir:4b825dc642cb6eb9a060e54bf8d69288fbee4904', # Empty Dir
    'swh:1:dir:75ed58f260bfa4102d0e09657803511f5f0ab372', # Empty subversion dir
    'swh:1:dir:7f780e0c9ca14660198f482330dff6dc48b7d3ee', # dir containing only 'test/' which is empty
]

def depth_first(stub, swhid, G):
    children = my_traverse(stub, [swhid], "*", "*",swhgraph_pb2.GraphDirection.BACKWARD, max_depth=0) 
    print("NODE")
    print(swhid)
    print("CHILDREN -----------------")
    print(children)
    return
    G.add_node()


def make_phylo_graph(stub, origin, depth=1, directories=False):
    G = nx.DiGraph()
    snps = my_traverse(stub, [origin], "ori:snp", "snp") # all snp from this ori
    snps = [snp.swhid for snp in snps]
    latest_revs = []
    heapq.heapify(latest_revs)
    revs = my_traverse(stub, snps, "snp:rev", "rev") # all rev from this ori (might be faster but is ok)
    for rev in revs:
        heapq.heappush(latest_revs, (rev.rev.author_date, rev.swhid)) # date first bc heapify uses tuple[0]
        if len(latest_revs) > depth:
            heapq.heappop(latest_revs)[0]
    latest_revs = sorted(latest_revs, reverse=True)
    latest_rev_swhid = latest_revs[0][1]
    G.add_node(latest_rev_swhid)
    return depth_first(stub, latest_rev_swhid, G)


    G.add_edge("A", "B")
    G.add_edges_from([("B", "C"), ("C", "D")])

    nx.draw(G, with_labels=True, node_color='lightblue', font_weight='bold')
    plt.show()

def find_migration(stub, origin, url='', depth=1, directories=False, no_snp=[], no_rev=[], no_dir=[]):
    snps = my_traverse(stub, [origin], "ori:snp", "snp") # all snp from this ori
    snps = [snp.swhid for snp in snps]
    if len(snps) == 0:
        n = get_node(stub, origin)
        no_snp.append(n)
        return []
    latest_revs = []
    heapq.heapify(latest_revs)
    revs = my_traverse(stub, snps, "snp:rev", "rev") # all rev from this ori (might be faster but is ok)
    if len(revs) == 0:
        n = get_node(stub, origin)
        no_rev.append(n)
        return []
    for rev in revs:
        heapq.heappush(latest_revs, (rev.rev.author_date, rev.swhid)) # date first bc heapify uses tuple[0]
        if len(latest_revs) > depth:
            heapq.heappop(latest_revs)[0]
    latest_revs = sorted(latest_revs, reverse=True)
    latest_swhids = [rev[1] for rev in latest_revs if rev[1] not in BAD_SWHID]
    if directories:
        dirs = my_traverse(stub, latest_swhids, "rev:dir", "dir")
        if len(dirs) > len(latest_swhids):
            print('HEEEERE' + url)
        latest_swhids = [dir.swhid for dir in dirs if dir.swhid not in BAD_SWHID]
    
    oris = my_traverse(stub, latest_swhids, "*", "ori",
                            swhgraph_pb2.GraphDirection.BACKWARD)
    return oris

