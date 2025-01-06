import grpc
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
from content_reader import sha1list_to_contents
from transformers import AutoTokenizer, AutoModel
from pyspark.sql import SparkSession
from utils import my_traverse
import torch.nn.functional as F
import time 
import tqdm
import chromadb
from chromadb.config import Settings


def traversal(stub, dir, languages_to_swhids, visited):
    node = stub.GetNode(swhgraph_pb2.GetNodeRequest(swhid=dir))
    if node.swhid in visited:
        return
    visited.add(node.swhid)
    for succ in node.successor:
        if len(succ.label) != 0:
            label = succ.label[0] # Only taking the first edge as the graph can be multi edge
            try:
                name = label.name.decode('utf-8')
            except:
                continue
            split = name.split('.')
            if len(split) > 1: # there is an extension, it is likely a file
                ext = split[-1]
                swhids = languages_to_swhids.get(ext, None)
                if swhids is not None:
                    swhids.add(succ.swhid)
            else:
                traversal(stub, succ.swhid, languages_to_swhids, visited)


def populate(stub, ori, languages_to_files, visited):
    revs = my_traverse(stub, [ori], "ori:snp,snp:rev", "rev")
    latest_rev = None
    for rev in revs:
        if latest_rev == None or rev.rev.author_date > latest_rev.rev.author_date:
            latest_rev = rev
    if latest_rev == None:
        print("AYOO")
        print(ori)
        return
    root_dir = my_traverse(stub, [latest_rev.swhid], "rev:dir", "dir")
    if len(root_dir) > 0:
        root_dir = root_dir[0].swhid
        traversal(stub, root_dir, languages_to_files, visited) 


def read_origins(path):
    with open(path, 'r') as file:
        lines = [line.strip() for line in file]
    return lines


def set_sha1(df, swhids, collection):
    target_values = [swhid.split(':')[3] for swhid in swhids]
    target_df = spark.createDataFrame(target_values, ["sha1_git"])

    print('calling join')
    result = df.join(target_df, on="sha1_git", how="inner")
    print('join done')

    # Collect only the required data to the driver
    sha1_values = result.select("sha1", "sha1_git").collect()
    existing_ids = collection.get()["ids"]
    for row in tqdm.tqdm(sha1_values):
        id_ = 'swh:1:cnt:'+ row["sha1_git"]
        sha1 = row["sha1"]
        if id_ in existing_ids:
            print(f"ID {id_} already exists, skipping.")
            continue
        try:
            collection.add(
                ids=[id_],  # Single ID
                embeddings=[[0] * 768],  # Single embedding
                metadatas=[{"sha1": sha1}]  # Single metadata
            )
        except ValueError as e:
            print('error')


if __name__ == '__main__':
    channel = grpc.insecure_channel('localhost:50091')
    stub = swhgraph_pb2_grpc.TraversalServiceStub(channel)
    path = "/work/rayan.mostovoi/pythonCompressed/sources.txt"
    origins = read_origins(path)

    chroma_db_path = "/masto/rayan.mostovoi/chromadb_embeddings"
    orc_files_path = "/work/rayan.mostovoi/PythonOrc/content/"
    # Configure ChromaDB to use persistent storage
    client = chromadb.PersistentClient(path=chroma_db_path)

    # Create or load a collection
    collection_name = "embeddings"
    collection = client.get_or_create_collection(name=collection_name)
    # Connect to the Spark Connect server
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    #df = spark.read.format("orc").load('/masto/rayan.mostovoi/2024-05-16/ORC/content/').select("sha1", "sha1_git")
    df = spark.read.format("orc").load(orc_files_path).select("sha1", "sha1_git")

    languages = ['py']
    languages_to_swhids : dict[str, set[str]] = {language: set() for language in languages}
    print('Searching for important swhids')
    visited = set()
    for ori in origins:
        populate(stub, ori, languages_to_swhids, visited) # add all files in the ori to the languages_to_swhid dict
    
    swhids = sum([list(v) for v in languages_to_swhids.values()], [])

    print('Setting associated sha1')
    set_sha1(df, swhids, collection)