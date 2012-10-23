#!/bin/bash
#PBS -N katts-sample
#PBS -l nodes=1:ppn=23
#PBS -l walltime=00:60:00,mem=55000mb
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

pbsdsh -u "/home/user/hunziker/katts/submission_environment/katts_jobs/sample/tmp/node-script.sh" 

EVALUATION_FOLDER="/home/user/hunziker/katts/submission_environment/katts_jobs/sample/evaluation"
COMPLEATION_FILE="$EVALUATION_FOLDER/completed_on"

# Monitor the compleation file, if it is written, then we can continue
while [ ! -f "$COMPLEATION_FILE" ]; do
	sleep 15
done

# Give the processes sometime to shutdown, before copy the data.
sleep 10

pbsdsh -u "/home/user/hunziker/katts/submission_environment/katts_jobs/sample/tmp/evaluation-data-copy.sh" 

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


java -jar "/home/user/hunziker/katts/submission_environment/evaluation.jar" --google-username "katts.evaluation@gmail.com" --google-password "kattsevaluationongdocs" --google-spreadsheet-name "sample_experiment" --job-name "sample" "$EVALUATION_FOLDER"


echo "Done!"



