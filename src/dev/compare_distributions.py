#!/usr/bin/python
#
# This script compares the partitioning of nodes computed by Metis with a random distribution. The script
# takes three arguments: the first being the number of servers to simulate, the second being
# the json file containing the sending behavior of the tasks, and the third being the partitioning as it 
# was computed by Metis. Example:
#
#       ./compare_distributions.py 12 sendgraph.json sendgraph.metis.part.12
#

import sys
import json

# abort if required options are missing
if len(sys.argv) < 4:
    print("You need to specify the number of servers in the cluster, a json file, and the metis partition file.\n"
          " Usage: compare_distributions.py [numberOfServers] [json file] [partition file]")
    exit(0)

number_of_Servers = int(sys.argv[1])

# read json file
with open(sys.argv[2]) as json_file:
    j = json.loads(json_file.read())
    jNodes = j['nodes']
    jLinks = j['links']

# read metis partition
task_assignment = [0 for x in range(len(jNodes))]
with open(sys.argv[3]) as metis_file:
    vertexId = 0
    for line in metis_file.readlines():
        task_assignment[vertexId] = int(line)
        vertexId += 1

# compute network traffic
random_traffic = 0
partitioned_traffic = 0
for link in jLinks:
    sourceId = int(link['source'])
    targetId = int(link['target'])
    value = int(link['value'])
    # random assignment
    if (sourceId % number_of_Servers) != (targetId % number_of_Servers): random_traffic += value
    # optimized assignment
    if task_assignment[sourceId] != task_assignment[targetId]: partitioned_traffic += value

print "random traffic: {0}\n" \
      "partitioned traffic: {1}\n" \
      "improvement: {2}".format(random_traffic, partitioned_traffic, float(1.0 * random_traffic / partitioned_traffic))