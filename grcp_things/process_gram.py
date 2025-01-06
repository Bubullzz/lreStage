import chromadb
import torch.nn.functional as F
import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import numpy as np
from utils import *
from get_relevant_sha import read_origins, populate, traversal
import torch
from tqdm import tqdm
import pandas as pd

def ori_to_cnts(stub, ori):
    revs = my_traverse(stub, [ori], "ori:snp,snp:rev", "rev")
    latest_rev = None
    for rev in revs:
        if latest_rev == None or rev.rev.author_date > latest_rev.rev.author_date:
            latest_rev = rev
    if latest_rev == None:
        return []
    cnts = my_traverse(stub, [latest_rev.swhid], "rev:dir,dir:dir,dir:cnt", "cnt")
    return [cnt.swhid for cnt in cnts]


if __name__ == "__main__":
    channel = grpc.insecure_channel('localhost:50091')
    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)
    # Initialize ChromaDB client and collection
    client = chromadb.PersistentClient(path="/masto/rayan.mostovoi/chromadb_embeddings")
    collection_name = "embeddings"
    collection = client.get_or_create_collection(name=collection_name)

    origins = read_origins("/work/rayan.mostovoi/pythonCompressed/sources.txt")
    file_sims : dict[str, float] = {} # swhid1_swhid2 -> similarity
    results = collection.get(where={}, include=["embeddings"])
    ids = results["ids"]
    embeddings = results["embeddings"]

    ori_to_embeddings = [] # list of embeddings for each ori, in the order of origins
    for ori in tqdm(origins, desc='associating embeddings for each ori'):
        cnt_swhids = ori_to_cnts(stub, ori)
        ori_embeddings = []
        for swhid in cnt_swhids:
            try:
                ori_embeddings.append(embeddings[ids.index(swhid)])
            except:
                continue
        ori_to_embeddings.append(ori_embeddings)

    assert(len(ori_to_embeddings) == len(origins)) # one embedding list per ori
    
    i = 0
    while i < len(ori_to_embeddings): # remove empty embeddings and associated origins
        if ori_to_embeddings[i] == []:
            del ori_to_embeddings[i]
            del origins[i]
        else:
            i += 1

    similarities = np.zeros((len(origins), len(origins)))
    for i in tqdm(range(len(origins)), desc="Computing origin similarities"):
        ori1 = origins[i]
        url_1 = get_node(stub, ori1).ori.url
        
        embeddings_1 = torch.tensor(np.stack(ori_to_embeddings[i]))
        for j in tqdm(range(i+1, len(origins)), leave=False, desc=(f'for {url_1}')):
            ori2 = origins[j]
            url_2 = get_node(stub, ori2).ori.url
            embeddings_2 = torch.tensor(np.stack(ori_to_embeddings[j]))
            all_similarities = torch.mm(F.normalize(embeddings_1, p=2, dim=1), F.normalize(embeddings_2, p=2, dim=1).T)
            max_similarities = all_similarities.max(dim=1).values
            res = sum(max_similarities) / len(max_similarities)
            similarities[i][j] = res
            similarities[j][i] = res
    
    urls = [get_node(stub, ori).ori.url for ori in origins]
    # Create a DataFrame for better labeling and handling
    df = pd.DataFrame(similarities, index=urls, columns=urls)

    # Save to a file (e.g., CSV)
    df.to_csv("similarity_matrix.csv") 

    """
    loaded_df = pd.read_csv("similarity_matrix.csv", index_col=0) 
    # Access data with labels
    print(loaded_df.loc["url1", "url2"])  # Access similarity between "url1" and "url2"
    print(loaded_df.loc["url1"])  # Access similarity scores for "url1" with all other URLs
    """
    #np.save("similarities.npy", similarities)
    #print(np.median(similarities)) # mediane de toooutes les sim (flatten automatique par median)