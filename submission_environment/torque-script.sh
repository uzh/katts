#!/bin/bash
#PBS -N katts-$EXPERIMENT
#PBS -l nodes=$NUMBER_OF_NODES:ppn=23
#PBS -l walltime=$WALLTIME,cput=2400000,mem=55000mb
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

#counter=0
#while [ "$counter" != "$NUMBER_OF_NODES" ] 
#do
#	pbsdsh -v -n "$counter" "$EXPERIMENT_TMP_FOLDER/node-script.sh" &
#	let counter=counter+1 
#done

echo "Try to distribute the scripts..."

nodes=`uniq $PBS_NODEFILE`

#echo $nodes

#for node in $nodes
#do
#	#echo $node
#	#pbsdsh -v -h "$node" "$EXPERIMENT_TMP_FOLDER/node-script.sh" &
#	pbsdsh -v -n "$node" "$EXPERIMENT_FOLDER/test.sh"
#done

echo "Scripts are distributed..."
pbsdsh -u "$EXPERIMENT_TMP_FOLDER/node-script.sh" 


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


