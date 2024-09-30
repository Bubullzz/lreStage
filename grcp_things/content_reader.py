import os
import pyarrow.orc as orc
import pyarrow as pa
import tqdm
import requests
import gzip
import io
import pandas as pd

def sha_read_and_update(file_path, sha1_git_values, sha1_values):
    try:
        with open(file_path, 'rb') as orc_file:
            orc_reader = orc.ORCFile(orc_file)
            table = orc_reader.read(['sha1_git', 'sha1'])
            df = table.to_pandas()
            sha1_git_remaining = pd.DataFrame(sha1_git_values)[pd.DataFrame(sha1_values) == ''] # Only keeping the sha1_git that don't already have a sha1 pair
            df = df[df['sha1_git'].isin(sha1_git_remaining[0])] # Magie noir but fait maison hehe, ça va vraiment optimiser les choses !!!!
            for i, sha1_git_value in enumerate(sha1_git_values):
                if sha1_values[i] == '':
                    result_row = df[df['sha1_git'] == sha1_git_value]
                    if not result_row.empty:
                        sha1_values[i] = result_row['sha1'].values[0]
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None


def sha1_git_to_sha1(directory_path, sha1_git_values):
    sha1_git_values = [v.split(':')[3] for v in sha1_git_values]
    sha1_values = ['' for _ in range(len(sha1_git_values))]
    for root, dirs, files in os.walk(directory_path):
        for file in tqdm.tqdm(files):
            file_path = os.path.join(root, file)
            sha_read_and_update(file_path, sha1_git_values, sha1_values)
            if '' not in sha1_values: # stop if we found all sha
                break
    return sha1_values

def get_file_content(sha):
    try:
        print('https://softwareheritage.s3.amazonaws.com/content/' + sha)
        response = requests.get('https://softwareheritage.s3.amazonaws.com/content/' + sha)
        if response.status_code == 200:
            file_content = response.content
            with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as gz:
                decompressed_content = gz.read()
            return decompressed_content.decode('utf-8')
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"An error occurred: {e}")