#!/bin/bash
#PBS -N katts-$KATTS_JOB
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

echo "Distribute scripts to the different nodes..."

pbsdsh -u "$KATTS_JOB_TMP_FOLDER/node-script.sh" 

EVALUATION_FOLDER="$KATTS_JOB_FOLDER/evaluation"
COMPLEATION_FILE="$EVALUATION_FOLDER/completed_on"

# Monitor the compleation file, if it is written, then we can continue
while [ ! -f "$COMPLEATION_FILE" ]; do
	sleep 15
done

# Give the processes sometime to shutdown, before copy the data.
sleep 10

pbsdsh -u "$KATTS_JOB_TMP_FOLDER/evaluation-data-copy.sh" 

# Give other nodes some time to copy the data. This is required, when the
# evaluation data is not distributed evenly.
sleep 20


# Aggregate the monitoring data from the different nodes
dataFolders=`ls "$EVALUATION_FOLDER/data"`

AGGREGATION_FOLDER="$EVALUATION_FOLDER/aggregates"

rm -rf "$AGGREGATION_FOLDER"
mkdir "$AGGREGATION_FOLDER"

# Debug output
echo -n "Start aggregating the data... "

	
for folder in $dataFolders
do
	filesInFolder=`ls "$EVALUATION_FOLDER/data/$folder"`
	
	for dataFile in $filesInFolder
	do
		cat "$EVALUATION_FOLDER/data/$folder/$dataFile" >> "$AGGREGATION_FOLDER/$dataFile"
	done
done


java -jar "$KATTS_HOME/scripts/evaluation.jar" --google-username "$GOOGLE_USERNAME" --google-password "$GOOGLE_PASSWORD" --google-spreadsheet-name "$GOOGLE_SPREADSHEET_NAME" --job-name "$KATTS_JOB" "$EVALUATION_FOLDER"


echo "Done!"



