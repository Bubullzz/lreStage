import os
import pyarrow.orc as orc
import pyarrow as pa
import tqdm
import requests
import gzip
import io
import pandas as pd

def sha_read_and_update(file_path, git_to_normal):
    try:
        with open(file_path, 'rb') as orc_file:
            table = orc.ORCFile(orc_file).read(['sha1_git', 'sha1'])
            df = table.to_pandas()
            print(df)
            sha1_git_remaining = [k for k,v in git_to_normal.items() if v == '']
            df = df[df['sha1_git'].isin(sha1_git_remaining)] # Magie noir but fait maison hehe, ça va vraiment optimiser les choses !!!!
            for sha1_git in sha1_git_remaining:
                result_row = df[df['sha1_git'] == sha1_git] # Very fast, congratulations
                if not result_row.empty:
                    git_to_normal[sha1_git] = result_row['sha1'].values[0]
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None


def sha1_git_to_sha1(directory_path, sha1_git_values):
    git_to_normal = {v.split(':')[3]: '' for v in sha1_git_values} # only keeping the sha part, skipping cnt:1:....
    for root, dirs, files in os.walk(directory_path):
        for file in tqdm.tqdm(files):
            file_path = os.path.join(root, file)
            sha_read_and_update(file_path, git_to_normal)
            if '' not in git_to_normal.values(): # stop if we found all sha
                break
    return git_to_normal

def get_file_content(sha):
    try:
        response = requests.get('https://softwareheritage.s3.amazonaws.com/content/' + sha)
        if response.status_code == 200:
            file_content = response.content
            with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as gz:
                decompressed_content = gz.read()
            return decompressed_content.decode('utf-8')
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")