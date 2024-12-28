import os
import pyarrow.orc as orc
import pyarrow as pa
import tqdm
import requests
import gzip
import io
import pandas as pd
import aiohttp
import asyncio
import random
def sha_read_and_update(file_path, git_to_normal):
    try:
        with open(file_path, 'rb') as orc_file:
            table = orc.ORCFile(orc_file).read(['sha1_git', 'sha1'])
            df = table.to_pandas()
            sha1_git_remaining = [k for k,v in git_to_normal.items() if v == '']
            df = df[df['sha1_git'].isin(sha1_git_remaining)] # Magie noir but fait maison hehe, ça va vraiment optimiser les choses !!!!
            for sha1_git in sha1_git_remaining:
                result_row = df[df['sha1_git'] == sha1_git] # Very fast, congratulations
                if not result_row.empty:
                    git_to_normal[sha1_git] = result_row['sha1'].values[0]
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None

def get_orc_as_dict(directory_path):
    db_dict = {}
    for root, dirs, files in os.walk(directory_path):
        for file in tqdm.tqdm(files, desc="making database into a dict"):
            file_path = os.path.join(root, file)
            with open(file_path, 'rb') as orc_file:
                df = orc.ORCFile(orc_file).read(['sha1_git', 'sha1']).to_pandas()
                db_dict.update(dict(zip(list(df['sha1_git']),list(df['sha1']))))
    return db_dict

def swhids_to_sha1(directory_path, swhids, db_dict):
    # takes a list of swhid as inputs
    # returns swhid_to_sha1
    if db_dict is None:
        db_dict = get_orc_as_dict(directory_path)
    return {v: db_dict.get(v.split(':')[3], '') for v in swhids}

async def async_get_file_content_with_retries(sha1, session: aiohttp.ClientSession, semaphore, retries=10):
    try:
        async with semaphore:  # Limit concurrent requests
            timeout = aiohttp.ClientTimeout(total=3)
            link = 'https://softwareheritage.s3.amazonaws.com/content/' + sha1
            for attempt in range(retries):
                try:
                    async with session.get(link, timeout=timeout) as response:
                        if response.status == 200:
                            file_content = await response.read()  # Non-blocking read
                            with gzip.GzipFile(fileobj=io.BytesIO(file_content)) as gz:
                                decompressed_content = gz.read()
                            return decompressed_content.decode('utf-8')
                    break  # Exit retry loop if successful chatgpt a dit donc je laisse
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    if attempt < retries - 1:
                        await asyncio.sleep(random.uniform(1, 3))  # Exponential backoff
    except Exception as e:
        return None
    return None

async def async_read_sha1(sha1_list, max_concurrent_requests=100, verbose=False):
    semaphore = asyncio.Semaphore(max_concurrent_requests)  # Limit number of concurrent requests
    async with aiohttp.ClientSession() as session:
        tasks = {
            s: asyncio.create_task(async_get_file_content_with_retries(s, session, semaphore)) 
            for s in sha1_list
        }
        results = await asyncio.gather(*tasks.values())  # Await all tasks concurrently
        sha1_to_content = {s: result for s, result in zip(tasks.keys(), results)}

    print(f"Total errors encountered: {sum(1 for result in results if result is None)}")  # Print the error count at the end
    return sha1_to_content
    

def sha1list_to_contents(sha1_list):
    # sha1 : a list of sha1
    return asyncio.run(async_read_sha1(sha1_list))

def swhids_to_contents(orc_path, swhids, db_dict=None):
    # orc_path : where is the sha_git_to_sha stored
    # swhids : list of swhids
    # careful, this function can put values as None
    swhids_to_sha1_res = swhids_to_sha1(orc_path, swhids, db_dict)
    sha1list_to_content_res = sha1list_to_contents(swhids_to_sha1_res.values())
    return {swhid: sha1list_to_content_res[swhids_to_sha1_res[swhid]]
            for swhid in swhids
            }



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