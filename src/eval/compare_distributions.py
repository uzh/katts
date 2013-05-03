#!/usr/bin/python
#
# This script compares the partitioning of nodes computed by Metis with a random distribution. The script
# takes three arguments: the first being the number of servers to simulate, the second being
# the json file containing the sending behavior of the tasks, and the third being the partitioning as it 
# was computed by Metis. Example:
#
#       ./compare_distributions.py 12 sendgraph.json sendgraph.metis.part.12
#

import optparse
import sys
import json
import eval

usage = "Usage: %prog [options] numberOfServers json file partition file"
description = "This script compares the partitioning of nodes computed by Metis with a random distribution."

parser = optparse.OptionParser(usage=usage,description=description)
parser.add_option("-l", "--light", dest="mode_light", action="store_true", default=False,
                  help="Light mode will print just two values being the two percentage values of messages that are "
                       "over the network. Example: 0.95,0.7")
(options, args) = parser.parse_args()

# abort if required options are missing
if len(args) < 3:
    parser.print_help()
    exit(1)

mode_light = options.mode_light

number_of_Servers = int(args[0])

# read json file
with open(args[1]) as json_file:
    j = json.loads(json_file.read())
    jNodes = j['nodes']
    jLinks = j['links']

# read task assignment from metis file
num_vertices = len(jNodes)
task_assignment = eval.read_metis_partition(args[2])

# compute network traffic
(total_messages, uniform_messages, partitioned_messages, uniform_server_load, partitioned_server_load) \
    = eval.compare_distributions(jLinks, task_assignment, number_of_Servers)

uniform_traffic = float(1.0 * uniform_messages / total_messages)
partitioned_traffic = float(1.0 * partitioned_messages / total_messages)

if mode_light:
    print ",".join((str(uniform_traffic), str(partitioned_traffic)))
else:
    print "total messages:      {0}\n" \
          "uniform traffic:     {1}({2:.2%})\n" \
          "partitioned traffic: {3}({4:.2%})\n" \
          "improvement:         {5:.2%}".format(total_messages,
                                                uniform_messages, uniform_traffic,
                                                partitioned_messages, partitioned_traffic,
                                                1-float(1.0 * partitioned_messages / uniform_messages))

    (uniform_mean, uniform_stdv) = eval.meanstdv(uniform_server_load)
    uniform_relstdv = uniform_stdv / uniform_mean
    (partitioned_mean, partitioned_stdv) = eval.meanstdv(partitioned_server_load)
    partitioned_relstdv = partitioned_stdv / partitioned_mean

    print "uniform load:        {0}\n" \
          "mean, stdv, relstdv: {1:.2f},{2:.2f},{3:.2%}\n" \
          "partitioned load:    {4}\n" \
          "mean, stdv, relstdv: {5:.2f},{6:.2f},{7:.2%}\n".format(uniform_server_load,
                                           uniform_mean, uniform_stdv, uniform_relstdv,
                                           partitioned_server_load,
                                           partitioned_mean, partitioned_stdv, partitioned_relstdv)
