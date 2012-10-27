#!/bin/bash
# 
# Author: Thomas Hunziker
#
# This script submits a job to the kraken cluster. Each job has his own expirment directory in
# the home directory for shared data. The direcotry must contain the jar to use and the query to 
# execute. Also additional parameters can be defined, which will be passed to the executed jar. 
# The bootstrap.sh in the katts_job folder is executed before the job is submitted. This can
# be used to set some indiviual options or per job actions.
# 

# Check arguments
if [ ! "$#" -eq "1" ] && [ ! "$#" -eq "2" ]
then
	echo 'Usage: job.sh katts_job-folder-name [Debug]';
	echo 'The first parameter must be set to a katts_job folder name.';
	exit;
fi


DEBUG="False";
if [ "$2" == "debug" ]
then
	DEBUG="True"
fi

KATTS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$KATTS_HOME"

KATTS_JOB=$1;
KATTS_JOBS_FOLDER="$KATTS_HOME/katts_jobs";
KATTS_JOB_FOLDER="$KATTS_JOBS_FOLDER/$KATTS_JOB";


if [ ! -d "$KATTS_JOB_FOLDER" ]; then
	echo "The given katts_job folder does not exists. (Folder path: '$KATTS_JOB_FOLDER')"
	exit;
fi

TOPOLOGY_JAR="$KATTS_JOB_FOLDER/katts.jar"

if [ ! -f "$TOPOLOGY_JAR" ]; then
	echo "The given katts_job folder does not contain a 'katts.jar'. (Path: '$TOPOLOGY_JAR')"
	exit;
fi

QUERY_FILE="$KATTS_JOB_FOLDER/query.xml"

if [ ! -f "$QUERY_FILE" ]; then
	echo "The given katts_job folder does not contain a 'query.xml'. (Path: '$QUERY_FILE')"
	exit;
fi


ADDITIONAL_PARAMETERS=""
TOPOLOGY_DEPLOYMENT_CLASS_NAME="ch.uzh.ddis.katts.RunXmlQuery"
NUMBER_OF_NODES="2"
WALLTIME="12:00:00"

KATTS_JOB_TMP_FOLDER="$KATTS_JOB_FOLDER/tmp"

# Clean the katts_jobs folder:
rm -rdf "$KATTS_JOB_TMP_FOLDER"

# Create Tmp folder
if [ ! -d "$KATTS_JOB_TMP_FOLDER" ]; then
	mkdir "$KATTS_JOB_TMP_FOLDER"
fi

BOOTSTRAP_FILE="$KATTS_JOB_FOLDER/bootstrap.sh"

# Include the bootstrap file
if [ -f "$BOOTSTRAP_FILE" ]; then
	. $BOOTSTRAP_FILE
fi

# Variable that are exportet to the torque script (variable names should not contain whitespaces):
variables="KATTS_JOB_TMP_FOLDER KATTS_HOME KATTS_JOB_FOLDER KATTS_JOB TOPOLOGY_JAR QUERY_FILE ADDITIONAL_PARAMETERS NUMBER_OF_NODES WALLTIME DEBUG TOPOLOGY_DEPLOYMENT_CLASS_NAME STARTUP_UI NODE_PROCESSOR_LIST NUMBER_OF_PROCESSOR_RESERVED_PER_NODE EXPECTED_MEMORY_CONSUMPTION GOOGLE_USERNAME GOOGLE_PASSWORD GOOGLE_SPREADSHEET_NAME"

# Create the torque script:
cp "scripts/torque-script.sh" "$KATTS_JOB_TMP_FOLDER"

for variable in $variables
do
	variableName="value=\$$variable"
	eval $variableName
	
	sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$KATTS_JOB_TMP_FOLDER/torque-script.sh"
done


# Create the node script:
cp "scripts/node-script.sh" "$KATTS_JOB_TMP_FOLDER"

for variable in $variables
do
	variableName="value=\$$variable"
	eval $variableName
	
	sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$KATTS_JOB_TMP_FOLDER/node-script.sh"
done


# Create the evaluation data copy script
cp "scripts/evaluation-data-copy.sh" "$KATTS_JOB_TMP_FOLDER"

for variable in $variables
do
	variableName="value=\$$variable"
	eval $variableName
	
	sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$KATTS_JOB_TMP_FOLDER/evaluation-data-copy.sh"
done


chmod -R 0777 "$KATTS_JOB_FOLDER"

if [ "$DEBUG" == "False" ]
then
	# Submit the created torque script
	mkdir "$KATTS_JOB_TMP_FOLDER/torque-log"
	cat "$KATTS_JOB_TMP_FOLDER/torque-script.sh" | qsub -d "$KATTS_JOB_TMP_FOLDER/torque-log"
	
else
	bash "$KATTS_JOB_TMP_FOLDER/torque-script.sh"
fi
