#!/usr/bin/python
#
# This script takes a file path as an argument argument and converts its json content into a graph
# structure that is readable by Metis.
#
# You can run the output of this file through Metis with the following command
#
#   gpmetis -tpwgts=12.tpwgts sendgraph.metis 12
#
# where the graph1.6.tpwgts file contains target weights for each partition
#

import sys
import json
import collections

# abort if required options are missing
if len(sys.argv) < 2:
    print("You need to specify a json file to read.\n Usage: json2metis.py [json file]")
    exit(0)

json_file = open(sys.argv[1], 'r')
j = json.loads(json_file.read())
jNodes = j['nodes']
jLinks = j['links']
num_nodes = len(jNodes)
num_links = len(jLinks)
link_weights = collections.defaultdict(list) # a list of target-value tuples, the targetId is 1-based
vertex_weights = collections.defaultdict(lambda: 1) # default weight of 1

# build the connection matrix
for link in jLinks:
    sourceId = int(link['source'])
    targetId = int(link['target'])
    value = int(link['value'])
    link_weights[sourceId].append((targetId, value))
    link_weights[targetId].append((sourceId, 1)) # reverse link with a weight of 1
    vertex_weights[targetId] += value

# print header
print "{0} {1} 011 1".format(num_nodes, num_links)

for sourceId in xrange(num_nodes):
    sys.stdout.write("{0}".format(vertex_weights[sourceId]))
    for outgoingLink in link_weights[sourceId]:
        sys.stdout.write(" {0} {1}".format(outgoingLink[0]+1, outgoingLink[1])) # Metis index is 1-based (not 0-based)
    sys.stdout.write("\n")
