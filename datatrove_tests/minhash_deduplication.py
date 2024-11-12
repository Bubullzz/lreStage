from datatrove.executor.slurm import SlurmPipelineExecutor
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup import MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import (
    MinhashConfig,
    MinhashDedupBuckets,
    MinhashDedupCluster,
    MinhashDedupFilter,
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.data import Document
from url_reader import UrlReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.hashing import HashConfig
from datatrove.utils.typeshelper import Languages

if __name__ == '__main__':
    print('starting...')
    # you can also change ngrams or the number of buckets and their size here
    minhash_config = MinhashConfig(
        hash_config=HashConfig(precision=64),
        num_buckets=14,
        hashes_per_bucket=8,
    )  # better precision -> fewer false positives (collisions)

    BASE = "/home/tmp"
    TOTAL_TASKS = 128

    # this is the original data that we want to deduplicate

    url10 = "mini10_url_to_swhid"
    url100 = "mini100_url_to_swhid"
    urlAll = "url_to_swhid"


    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    stage1 = LocalPipelineExecutor(
        pipeline=[
            UrlReader(f"/home/lrhome/lreStage/urls"),
            MinhashDedupSignature(output_folder=f"{BASE}/signatures", config=minhash_config, language=Languages.english)
        ],
        tasks=TOTAL_TASKS,
        logging_dir=f"{BASE}/signatures",
        skip_completed=True
    )


    # stage 2 finds matches between signatures in each bucket
    stage2 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupBuckets(
                input_folder=f"{BASE}/signatures",
                output_folder=f"{BASE}/buckets",
                config=minhash_config,
            ),
        ],
        tasks=minhash_config.num_buckets,
        logging_dir=f"{BASE}/buckets",
        depends=stage1,
        skip_completed=True
    )


    stage3 = LocalPipelineExecutor(
        pipeline=[
            MinhashDedupCluster(
                input_folder=f"{BASE}/buckets",
                output_folder=f"{BASE}/remove_ids",
                config=minhash_config,
            ),
        ],
        tasks=1,
        logging_dir=f"{BASE}/clusters",
        depends=stage2,
        skip_completed=False
    )
    stage3.run()

"""
    # stage 4 reads the original input data and removes all but 1 sample per duplicate cluster
    # the data must match exactly stage 1, so number of tasks and the input source must be the same
    stage4 = SlurmPipelineExecutor(
        job_name="mh4",
        pipeline=[
            INPUT_READER,
            TokensCounter(),  # nice way to see how many tokens we had before and after deduplication
            MinhashDedupFilter(
                input_folder=f"{S3_MINHASH_BASE_PATH}/remove_ids",
                exclusion_writer=JsonlWriter(f"{S3_MINHASH_BASE_PATH}/removed"),
            ),
            JsonlWriter(output_folder=f"{S3_MINHASH_BASE_PATH}/deduplicated_output"),
        ],
        tasks=TOTAL_TASKS,
        time="50:00:00",
        partition="hopper-cpu",
        logging_dir=f"{S3_LOGS_FOLDER}/filter",
        depends=stage3,
        slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/filter/slurm_logs",
    )
    
    
    stage4.run()
"""
