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


def rec_get_files(stub, swhid, name_to_swhid, path, visited, extension):
    if swhid in visited: # Already visited this node
        return
    visited.add(swhid)
    if any(path.split('.')[-1] == ext for ext in extension): # enough to check it we are on a cnt node
        name_to_swhid.setdefault(path, set()) # if not exist initialize empty set, then append swhid
        name_to_swhid[path].add(swhid)
        return 
    
    node = stub.GetNode(swhgraph_pb2.GetNodeRequest(swhid=swhid))
    for succ in node.successor:
        if len(succ.label) != 0:
            label = succ.label[0] # Only taking the first edge as the graph can be multi edge
            try:
                name = label.name.decode('utf-8')
            except:
                return
            new_path = f"{path}/{name}"
            rec_get_files(stub, succ.swhid, name_to_swhid, new_path, visited, extension) 


def dataset_maker(stub, all_origins, start_index=0, depth=2, extension=['c', 'cc', 'py'], out_dir='./rev2rev_dataset_V3'):
    """
    Generates a dataset of files with multiple versions from a set of origins.
    This function traverses through a set of origins to collect files and their versions,
    filters out files with only one version, and saves the remaining files with multiple
    versions into a specified directory.
    Args:
        stub: The gRPC stub used for making remote procedure calls.
        all_origins (list): A list of origin identifiers to start the traversal.
        dataset_size (int, optional): Not used for the moment
        depth (int, optional): The maximum number of revisions we go between. Will probably bug if > 2
    Returns:
        int: the number of pairs created from this list of origins
    Notes:
        - The function uses a heap to keep track of the latest versions of files.
        - Files with only one version are removed from the final dataset.
        - The resulting dataset is saved in the out_dir
    """

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
            root_dir = my_traverse(stub, [swhid], "rev:dir", "dir")
            if len(root_dir) == 0:
                continue
            root_dir = root_dir[0]
            swhid = root_dir.swhid
            rec_get_files(stub, swhid, name_to_swhid, f"{iteration}", visited, extension)
        iteration += 1


    print(f"number of files found : {len(name_to_swhid)}")

    # removing all files with only one version
    name_to_swhid = {name:ids for name, ids in name_to_swhid.items() if len(ids) >= depth} 
    print(f"after keeping only multi-version files: {len(name_to_swhid)}")
    
    all_swhids = [swhid for swhids_set in name_to_swhid.values() for swhid in swhids_set]
    directory_path = '/masto/rayan.mostovoi/2024-05-16/ORC/content/'
    db_dict = get_orc_as_dict(directory_path)
    swhids_to_contents_res = swhids_to_contents(directory_path, all_swhids, db_dict=db_dict)

    name_to_contents = {name: [swhids_to_contents_res[swhid] for swhid in name_to_swhid[name] 
                               if swhids_to_contents_res[swhid] != None]
                        for name
                        in name_to_swhid.keys()}
    
    # remove all files with only one version after losses from http request
    name_to_contents = {name:contents for name,contents in name_to_contents.items() if len(contents) > 1}
    print(f"after removing Nones : {len(name_to_contents)}")

    i = start_index
    for name, contents in name_to_contents.items():
        path = os.path.join(out_dir,f"set_{i}")
        os.mkdir(path)
        for j, cnt in enumerate(contents):
            with open(os.path.join(path,f"V_{j}"), 'w') as file:
                file.write(cnt)
        with open(os.path.join(path,"info.txt"), 'w') as file:
            file.write(name)
        i += 1
    return i
