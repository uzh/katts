#!/usr/bin/python
#
# This script takes one or multiple json files as an argument argument and converts its content into a graph
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
    print("You need to specify at least one json file to read.\n Usage: json2metis.py json_file0 [json_file1...n]")
    exit(0)

#  default is a dictionary of a default dictionary with a default weight of 0
link_weights = collections.defaultdict(lambda: collections.defaultdict(lambda: 0)) # default value is 0
vertex_weights = collections.defaultdict(lambda: 1) # default weight of 1 (lorenz: why 1?)
max_node_id = 0

for json_filepath in sys.argv[1:]: # first element in arguments list is the name of the app
    with open(json_filepath, 'r') as json_file:
        j = json.loads(json_file.read())
        jLinks = j['links']

        # build the connection matrix
        for link in jLinks:
            sourceId = int(link['source'])
            targetId = int(link['target'])
            value = int(link['value'])

            if sourceId > max_node_id:
                max_node_id = sourceId
            if targetId > max_node_id:
                max_node_id = targetId

            link_weights[sourceId][targetId] += value
            # metis requires a reverse link, so we put a value of 1 even when added multiple times
            link_weights[targetId][sourceId] = 1
            vertex_weights[targetId] += value

# count nodes and links
num_nodes = max_node_id + 1 # index starts at 0
num_links = 0
for source_id in link_weights.keys():
    num_links += len(link_weights[source_id].keys())
num_links = num_links / 2 # metis does not support undirected graphs, and looks at the links as one

# print header (fix format of 011 1)
print "{0} {1} 011 1".format(num_nodes, num_links)

# print links
for sourceId in xrange(num_nodes):
    sys.stdout.write("{0}".format(vertex_weights[sourceId]))
    sorted_target_ids = link_weights[sourceId].keys()
    sorted_target_ids.sort()
    for target_id in sorted_target_ids:
        link_weight =  link_weights[sourceId][target_id]
        # first element is target id, second element is weight
        sys.stdout.write(" {0} {1}".format(target_id+1, link_weight)) # Metis index is 1-based (not 0-based)
    sys.stdout.write("\n")
