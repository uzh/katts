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

"""
Calculate mean and standard deviation of data x[]:
    mean = {\sum_i x_i \over n}
    std = sqrt(\sum_i (x_i - mean)^2 \over n-1)

Copied from: http://www.physics.rutgers.edu/~masud/computing/WPark_recipes_in_python.html
"""
def meanstdv(x):
    from math import sqrt
    n, mean, std = len(x), 0, 0
    for a in x:
        mean = mean + a
    mean = mean / float(n)
    for a in x:
        std = std + (a - mean)**2
    std = sqrt(std / float(n-1))
    return mean, std

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
total_messages = 0
uniform_traffic = 0
partitioned_traffic = 0
uniform_server_load = [0 for x in range(number_of_Servers)]
partitioned_server_load = [0 for x in range(number_of_Servers)]
for link in jLinks:
    sourceId = int(link['source'])
    targetId = int(link['target'])
    value = int(link['value'])
    total_messages += value
    # random assignment
    if (sourceId % number_of_Servers) != (targetId % number_of_Servers): uniform_traffic += value
    # optimized assignment
    if task_assignment[sourceId] != task_assignment[targetId]: partitioned_traffic += value
    # compute load
    uniform_server_load[targetId % number_of_Servers] += value
    partitioned_server_load[task_assignment[targetId]] += value

print "total messages:      {0}\n" \
      "uniform traffic:     {1}({2:.2%})\n" \
      "partitioned traffic: {3}({4:.2%})\n" \
      "improvement:         {5:.2%}".format(total_messages, 
                                        uniform_traffic, float(1.0 * uniform_traffic / total_messages),
                                        partitioned_traffic, float(1.0 * partitioned_traffic / total_messages),
                                        1-float(1.0 * partitioned_traffic / uniform_traffic))

(uniform_mean, uniform_stdv) = meanstdv(uniform_server_load)
uniform_relstdv = uniform_stdv / uniform_mean
(partitioned_mean, partitioned_stdv) = meanstdv(partitioned_server_load)
partitioned_relstdv = partitioned_stdv / partitioned_mean

print "uniform load:        {0}\n" \
      "mean, stdv, relstdv: {1:.2f},{2:.2f},{3:.2%}\n" \
      "partitioned load:    {4}\n" \
      "mean, stdv, relstdv: {5:.2f},{6:.2f},{7:.2%}\n".format(uniform_server_load,
                                       uniform_mean, uniform_stdv, uniform_relstdv,
                                       partitioned_server_load,
                                       partitioned_mean, partitioned_stdv, partitioned_relstdv)