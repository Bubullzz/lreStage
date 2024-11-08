import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import sys
import tqdm
from utils import *
from property_checkers import *
from content_reader import *
from google.protobuf.json_format import MessageToDict


def rec_get_files(stub, swhid, name_to_swhid, path, visited, extension=['c', 'cc', 'py']):
    node = stub.GetNode(swhgraph_pb2.GetNodeRequest(swhid=swhid))
    for succ in node.successor:
        if succ.swhid in visited: # Already visited this node
            return
        visited.add(swhid)
        if len(succ.label) != 0:
            label = succ.label[0] # Only taking the first edge as the graph can be multi edge 
            name = label.name.decode('utf-8')
            name = f"{path}/{name}"
            if any(name.split('.')[-1] == ext for ext in extension):
                name_to_swhid.setdefault(name, set()) # if not exist initialize empty set, then append swhid
                name_to_swhid[name].add(succ.swhid)
            rec_get_files(stub, succ.swhid, name_to_swhid, name, visited, extension) 


def dataset_maker(stub, all_origins, dataset_size=5, depth=2):
    name_to_swhid = {} # linking each file name to all it's versions in the subset of selected revs. Might need to add a more comprehensive time ranking of the version later
    visited = set()
    iteration = 0
    for origin in tqdm.tqdm(all_origins, desc=f"map origin's files"):
        snps = my_traverse(stub,[origin], "ori:snp", "snp") # all snp from this ori
        snps = [snp.swhid for snp in snps]
        latest_revs = []
        heapq.heapify(latest_revs)
        revs = my_traverse(stub, snps, "snp:rev", "rev") # all rev from this ori (might be faster but is ok)

        for rev in revs:
            heapq.heappush(latest_revs, (rev.rev.author_date, rev.swhid)) # date first bc heapify uses tuple[0]
            if len(latest_revs) > depth:
                str(heapq.heappop(latest_revs)[0])
        latest_revs = sorted(latest_revs, reverse=True)
        for rev in latest_revs:
            swhid = rev[1]
            root_dir = my_traverse(stub, [swhid], "rev:dir", "dir")[0]
            swhid = root_dir.swhid
            rec_get_files(stub, swhid, name_to_swhid, f"{iteration}", visited)
        iteration += 1


    print(f"number of files found : {len(name_to_swhid)}")

    # removing all files with only one version
    name_to_swhid = {name:ids for name, ids in name_to_swhid.items() if len(ids) >= depth} 
    print(f"after keeping only multi-version files: {len(name_to_swhid)}")
    
    all_swhids = [swhid for swhids_set in name_to_swhid.values() for swhid in swhids_set]
    print(len(all_swhids), len(set(all_swhids)))
    directory_path = '/work/PythonOrc/content/'
    swhids_to_contents_res = swhids_to_contents(directory_path, all_swhids)

    name_to_contents = {name: [swhids_to_contents_res[swhid] for swhid in name_to_swhid[name] 
                               if swhids_to_contents_res[swhid] != None]
                        for name
                        in name_to_swhid.keys()}
    print(f"after removing Nones : {len(name_to_swhid)}")
    
    # remove all files with only one version after losses from http request
    name_to_contents = {name:contents for name,contents in name_to_contents.items() if len(contents) > 1}

    dir = './rev2rev_dataset_V2'
    for i, (name, contents) in enumerate(name_to_contents.items()):
        path = os.path.join(dir,f"set_{i}")
        os.mkdir(path)
        for j, cnt in enumerate(contents):
            with open(os.path.join(path,f"V_{j}"), 'w') as file:
                file.write(cnt)
    return name_to_contents



if __name__ == '__main__':
    run()
