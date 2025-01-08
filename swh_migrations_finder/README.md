# SWH Migration Finder

This repository contains everything needed to run the migration finder algorithm.

## Input Format

The algorithm takes as input pairs of `(swhid, url)` in the following format:
```bash
swhid: url
```


An example input file, `inria_pairs.txt`, is provided in the expected format.

## Prerequisites

The code requires an instance of the graph to be running on `localhost:50091`.

## Usage

To run the migration finder algorithm, execute the following command:

```bash
python main.py inria_pairs.txt
```

This will attempt to find all migrations from each given source.

## GitHub API Integration
If you want to use GitHub API calls to prune explicit GitHub forks, you need to add a GitHub token in utils.py.