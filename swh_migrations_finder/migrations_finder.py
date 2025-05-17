#
# Copyright (C) 2025 Bulle Mostovoi <bulle.mostovoi@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import chardet
import sys
import tqdm
import grpc
import math
import datetime
import heapq
import swhgraph_pb2
import swhgraph_pb2_grpc
from google.protobuf import field_mask_pb2
import networkx as nx

from utils import *


# List of swhids that are not relevants phylogeny-wise
BAD_SWHID = [
    'swh:1:dir:4b825dc642cb6eb9a060e54bf8d69288fbee4904', # Empty Dir
    'swh:1:dir:75ed58f260bfa4102d0e09657803511f5f0ab372', # Empty subversion dir
    'swh:1:dir:7f780e0c9ca14660198f482330dff6dc48b7d3ee', # dir containing only 'test/' which is empty
]


def find_migration(stub, origin, url='', depth=1, directories=False, check_github_fork=True):
    """
    Graph traversal to find the migrations of a given origin.

    First goes down to the `depth` latest revisions (or directories if set to True) of the origin, 
    Then performs a backward traversal from these nodes to find all the associated origins.
    Then performs a check on the GitHub API to see if the origin is a fork or not.
    """
    # Get the `depth` latest revisions from the origin
    latest_revs = []
    heapq.heapify(latest_revs)
    revs = my_traverse(stub, [origin], "ori:snp,snp:rev", "rev")
    if len(revs) == 0:
        return []
    for rev in revs:
        heapq.heappush(latest_revs, (rev.rev.author_date, rev.swhid)) # date first bc heapify uses tuple[0]
        if len(latest_revs) > depth:
            heapq.heappop(latest_revs)[0]
    latest_revs = sorted(latest_revs, reverse=True)
    latest_swhids = [rev[1] for rev in latest_revs if rev[1] not in BAD_SWHID]

    # If directories are requested, get the directories of the latest revisions
    if directories:
        dirs = my_traverse(stub, latest_swhids, "rev:dir", "dir")
        latest_swhids = [dir.swhid for dir in dirs if dir.swhid not in BAD_SWHID]
    
    # Perform a backward traversal from the latest revisions/directories to find all the associated origins
    oris = my_traverse(stub, latest_swhids, "*", "ori",
                            swhgraph_pb2.GraphDirection.BACKWARD)
    result = [(ori.ori.url, 2) for ori in oris] # 2 means don't know about migration status

    # Get actual fork status from GitHub API if requested
    if check_github_fork:
        result = [(ori,is_fork(ori))  for ori, _ in result]

    return result
