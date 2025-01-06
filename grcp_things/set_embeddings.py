import torch
from transformers import AutoTokenizer, AutoModel
import chromadb
from chromadb.config import Settings
from content_reader import sha1list_to_contents
import tqdm

def compute_embeddings(model, tokenizer, contents : list[str], max_length, device):
    tokens = tokenizer(contents, truncation=True, padding="max_length", max_length=max_length, return_tensors="pt")
    tokens = {k: v.to(device) for k, v in tokens.items()}
    return model(**tokens).last_hidden_state.mean(dim=1) # Oui, dim = 1

def update_db(swhids_sha_pairs, model, tokenizer, batch_size, max_length, device, force=False):
    metadata = {
        "model_name": model.config.name_or_path,
        "max_length": max_length,
    }
    while len(swhids_sha_pairs) > batch_size or (force and len(swhids_sha_pairs) > 0):
        batch = swhids_sha_pairs[:batch_size]
        del swhids_sha_pairs[:batch_size]
        swhids, shas = zip(*batch)
        swhids = list(swhids)
        contents = [v if v is not None else ""
            for v
            in sha1list_to_contents(shas).values()]
        embeddings = compute_embeddings(model, tokenizer, contents, max_length, device)
        embeddings = embeddings.detach().cpu()
        collection.update(
            ids=swhids,  
            embeddings=embeddings,
            metadatas=[metadata] * len(swhids)
        )
        

def process_chromadb_entries(collection, collection_traversal_batch_size, tokenizer, model, embedding_batch_size, max_length, device):
    """
    Reads entries from ChromaDB in batches, filters those with embedding [0], and computes new embeddings.
    """
    # Get all entries from the collection

    swhids_sha_pairs_to_embed = []

    for offset in tqdm.tqdm(range(0, collection.count(), collection_traversal_batch_size)):
        batch = collection.get(
            include=["metadatas", "embeddings"],
            offset=offset,
            limit=collection_traversal_batch_size
        )

        ids = batch["ids"]
        embeddings = batch["embeddings"]
        metadatas = batch["metadatas"]


        for id, embedding, metadata in zip(ids, embeddings, metadatas):
            if metadata.get("model_name") != model.config.name_or_path or metadata.get("max_length") != max_length or embedding[0] == 0:
                swhids_sha_pairs_to_embed.append((id, metadata.get("sha1")))
        
        update_db(swhids_sha_pairs_to_embed, model, tokenizer, embedding_batch_size, max_length, device, force=False)
    update_db(swhids_sha_pairs_to_embed, model, tokenizer, embedding_batch_size, max_length, device, force=True)
    

if __name__ == "__main__":
    # Initialize ChromaDB client and collection
    client = chromadb.PersistentClient(path="/masto/rayan.mostovoi/chromadb_embeddings")
    collection_name = "embeddings"
    collection = client.get_or_create_collection(name=collection_name)

    results = collection.get(where={}, include=["metadatas", "embeddings"], limit=5)

    # Set up PyTorch model and tokenizer
    model_name = "microsoft/codebert-base"
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if device == "cpu":
        print("No GPU found, falling back to CPU !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(device)

    # Process entries in batches
    embedding_batch_size = 128
    max_length = 512

    collection_traversal_batch_size = 1024
    print('everything loaded ! starting processing')
    process_chromadb_entries(collection, collection_traversal_batch_size, tokenizer, model, embedding_batch_size, max_length, device)