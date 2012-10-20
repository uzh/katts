#!/bin/bash
#PBS -N katts-$EXPERIMENT
#PBS -l nodes=$NUMBER_OF_NODES:ppn=$NUMBER_OF_PROCESSOR_RESERVED_PER_NODE
#PBS -l walltime=$WALLTIME,mem=$EXPECTED_MEMORY_CONSUMPTION
#PBS -j oe
#PBS -m b
#PBS -m e
#PBS -m a
#PBS -V

# This script is executed, when the job is started. This script is executed only once
# and not per node / machine.
# 
# Author: Thomas Hunziker
#

echo "Try to distribute the scripts..."

pbsdsh -u "$EXPERIMENT_TMP_FOLDER/node-script.sh" 

echo "Scripts are distributed..."


EVALUATION_FOLDER="$EXPERIMENT_FOLDER/evaluation"
COMPLEATION_FILE="$EVALUATION_FOLDER/completed_on"

# Monitor the compleation file, if it is written, then we can continue
while [ ! -f "$COMPLEATION_FILE" ]; do
	sleep 15
done


# Give other nodes some time to copy the data. This is required, when the
# evaluation data is not distributed evenly.
sleep 20

# Aggregate the monitoring data from the different nodes
dataFolders=`ls "$EVALUATION_FOLDER/data"`

AGGREGATION_FOLDER="$EVALUATION_FOLDER/aggregates"

rm -rf "$AGGREGATION_FOLDER"
mkdir "$AGGREGATION_FOLDER"

# Debug output
if [ "$DEBUG" == "True" ]
then
	echo "Start aggregating the data "
fi
	
for folder in $dataFolders
do
	filesInFolder=`ls "$EVALUATION_FOLDER/data/$folder"`
	
	for dataFile in $filesInFolder
	do
		cat "$EVALUATION_FOLDER/data/$folder/$dataFile" >> "$AGGREGATION_FOLDER/$dataFile"
	done
done

# TODO Calculate the aggregates and send them to the google spreadsheet 


