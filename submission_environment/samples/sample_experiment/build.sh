#!/bin/bash

#
# This script creates the different jobs for this experiment. 
#

# Create a job for one node, for two nodes and for three nodes.
listOfNodes=$(seq 1 3)

# Create for one processor per node, for two processors per node and so on a job.
listOfProcessors=$(seq 1 5)

# Build the jobs (in this example 5 x 3 = 15 jobs are created)
for nodes in $listOfNodes
do
	for processors in $listOfProcessors
	do
		jobPrefix="$nodes"_"$processors"
		
		let processorNumber=processorNumber - 1
		
		declare -A variables
		variables=( 
			["PROCESSORS"]="$processorNumber" 
			["NODES"]="$nodes" 
		)
		
		#echo "${variables["NODES"]}"
		
		create_job "$jobPrefix" "$KATTS_EXPERIMENT_FOLDER/job" "${variables}"
	done
done

