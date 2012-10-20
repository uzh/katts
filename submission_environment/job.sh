#!/bin/bash
# 
# Author: Thomas Hunziker
#
# This script submits a job to the kraken cluster. Each job has his own expirment directory in
# the home directory for shared data. The direcotry must contain the jar to use and the query to 
# execute. Also additional parameters can be defined, which will be passed to the executed jar. 
# The bootstrap.sh in the experiment folder is executed before the job is submitted. This can
# be used to set some indiviual options or per job actions.
# 

# Check arguments
if ( ${1} )
then
	echo 'Usage: job.sh experiment-folder-name [Debug]';
	echo 'The first parameter must be set to the experiment folder name.';
	exit;
fi

DEBUG="False";
if [ "$2" == "debug" ]
then
	DEBUG="True"
fi

KATTS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$KATTS_HOME"

EXPERIMENT=$1;
EXPERIMENTS_FOLDER="$KATTS_HOME/experiments";
EXPERIMENT_FOLDER="$EXPERIMENTS_FOLDER/$EXPERIMENT";


if [ ! -d "$EXPERIMENT_FOLDER" ]; then
	echo "The given experiment folder does not exists. (Folder path: '$EXPERIMENT_FOLDER')"
	exit;
fi

TOPOLOGY_JAR="$EXPERIMENT_FOLDER/katts.jar"

if [ ! -f "$TOPOLOGY_JAR" ]; then
	echo "The given experiment folder does not contain a 'katts.jar'. (Path: '$TOPOLOGY_JAR')"
	exit;
fi

QUERY_FILE="$EXPERIMENT_FOLDER/query.xml"

if [ ! -f "$QUERY_FILE" ]; then
	echo "The given experiment folder does not contain a 'query.xml'. (Path: '$QUERY_FILE')"
	exit;
fi


ADDITIONAL_PARAMETERS=""
TOPOLOGY_DEPLOYMENT_CLASS_NAME="ch.uzh.ddis.katts.RunXmlQuery"
NUMBER_OF_NODES="2"
WALLTIME="12:00:00"

EXPERIMENT_TMP_FOLDER="$EXPERIMENT_FOLDER/tmp"

# Clean the experiments folder:
rm -rdf "$EXPERIMENT_TMP_FOLDER"

# Create Tmp folder
if [ ! -d "$EXPERIMENT_TMP_FOLDER" ]; then
	mkdir "$EXPERIMENT_TMP_FOLDER"
fi

BOOTSTRAP_FILE="$EXPERIMENT_FOLDER/bootstrap.sh"

# Include the bootstrap file
if [ -f "$BOOTSTRAP_FILE" ]; then
	. $BOOTSTRAP_FILE
fi

# Variable that are exportet to the torque script (variable names should not contain whitespaces):
variables="EXPERIMENT_TMP_FOLDER KATTS_HOME EXPERIMENT_FOLDER EXPERIMENT TOPOLOGY_JAR QUERY_FILE ADDITIONAL_PARAMETERS NUMBER_OF_NODES WALLTIME DEBUG TOPOLOGY_DEPLOYMENT_CLASS_NAME STARTUP_UI NODE_PROCESSOR_LIST NUMBER_OF_PROCESSOR_RESERVED_PER_NODE EXPECTED_MEMORY_CONSUMPTION"

# Create the torque script:
cp "torque-script.sh" "$EXPERIMENT_TMP_FOLDER"

for variable in $variables
do
	variableName="value=\$$variable"
	eval $variableName
	
	sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$EXPERIMENT_TMP_FOLDER/torque-script.sh"
done

# Create the node script:
cp "node-script.sh" "$EXPERIMENT_TMP_FOLDER"

for variable in $variables
do
	variableName="value=\$$variable"
	eval $variableName
	
	sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$EXPERIMENT_TMP_FOLDER/node-script.sh"
done


chmod -R 0777 "$EXPERIMENT_FOLDER"

if [ "$DEBUG" == "False" ]
then
	# Submit the created torque script
	mkdir "$EXPERIMENT_TMP_FOLDER/torque-log"
	cat "$EXPERIMENT_TMP_FOLDER/torque-script.sh" | qsub -d "$EXPERIMENT_TMP_FOLDER/torque-log"
	
else
	bash "$EXPERIMENT_TMP_FOLDER/torque-script.sh"
fi


