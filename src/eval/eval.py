#!/usr/bin/python
#
# This file defines some functions that are used throughout the python evaluation.
#

__author__ = 'Lorenz Fischer'

def meanstdv(x):
    """ Calculate mean and standard deviation of data x[]:

        mean = {\sum_i x_i \over n}
        std = sqrt(\sum_i (x_i - mean)^2 \over n-1)

    Copied from: http://www.physics.rutgers.edu/~masud/computing/WPark_recipes_in_python.html
    """
    from math import sqrt
    n, mean, std = len(x), 0, 0
    for a in x:
        mean = mean + a
    mean = 1.0 * mean / float(n)
    for a in x:
        std = std + (a - mean)**2
    std = sqrt(std / float(n-1))
    return mean, std

def read_metis_partition(metis_file):
    """Reads the partition file created by metis and converts it into a dictionary each vertex (key)
        is assigned a partition (value).
    """
    # read metis partition
    task_assignment = dict()
    with open(metis_file) as metis_file:
        vertexId = 0
        for line in metis_file.readlines():
            task_assignment[vertexId] = int(line)
            vertexId += 1
    return task_assignment

def compare_distributions(send_graph_json, metis_partition, num_partitions):
    """Computes the amount of inter-machine packets for both, the partitioned case and the uniformly distributed
       case.

       Returns: (total_messages,
                 network_messages_uniform,
                 network_messages_partitioned,
                 server_load_uniform,
                 server_load_partitioned)
    """
    total_messages = 0
    uniform_traffic = 0
    partitioned_traffic = 0
    uniform_server_load = [0 for x in range(num_partitions)]
    partitioned_server_load = [0 for x in range(num_partitions)]

    for link in send_graph_json:
        sourceId = int(link['source'])
        targetId = int(link['target'])
        value = int(link['value'])
        total_messages += value
        # random assignment
        source_uniform = sourceId % num_partitions
        target_uniform = targetId % num_partitions
        if source_uniform != target_uniform:
            uniform_traffic += value

        # optimized assignment
        if sourceId in metis_partition:
            source_partition = metis_partition[sourceId]
        else:
            #print "ouch, no partition assigned to source id " + str(sourceId) + " using uniform"
            source_partition = target_uniform
        if targetId in metis_partition:
            target_partition = metis_partition[targetId]
        else:
            #print "ouch, no partition assigned to target id " + str(targetId) + " using uniform"
            target_partition = target_uniform
        if source_partition != target_partition:
            partitioned_traffic += value

        # compute load
        uniform_server_load[targetId % num_partitions] += value
        partitioned_server_load[target_partition] += value
    return total_messages, uniform_traffic, partitioned_traffic, uniform_server_load, partitioned_server_load
