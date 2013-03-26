#!/usr/bin/python
#
# This script compares the partitioning of nodes computed by Metis with a random distribution. The script
# takes two attributes, the first being the json file containing the sending behavior of the tasks and the
# second being the partitioning as it was computed by Metis.
#

import sys
import json

# abort if required options are missing
if len(sys.argv) < 2:
    print("You need to specify the number of servers in the cluster, a json file, and the metis partition file.\n"
          " Usage: compare_distributions.py [numberOfServers] [json file] [partition file]")
    exit(0)

# read json file
with open(sys.argv[1]) as json_file:
    j = json.loads(json_file.read())
    jNodes = j['nodes']
    jLinks = j['links']

# compute network traffic
total_traffic = 0
for link in jLinks:
    sourceId = int(link['source'])
    targetId = int(link['target'])
    total_traffic += int(link['value'])
   
print "overall traffic: {0}".format(total_traffic)