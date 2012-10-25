#!/bin/bash


KATTS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$KATTS_HOME"


. "$KATTS_HOME/scripts/experiments-functions.sh"


# Check arguments
if [ ! "$#" -eq "2" ]
then
	usage
fi

if [ ! "$1" == "run" ] && [ ! "$1" == "build" ]
then
	usage
fi

KATTS_EXPERIMENT_FOLDER="$KATTS_HOME/experiments/$2"
if [ ! -d "$KATTS_EXPERIMENT_FOLDER" ]; then
	echo "The given experiment folder does not exists. (Folder path: '$KATTS_EXPERIMENT_FOLDER')"
	exit;
fi

# Define some global variables
KATTS_JOB_FOLDER="$KATTS_HOME/katts_jobs"


if [ "$1" == "run" ]
then
	run_experiment "$2" 
fi

if [ "$1" == "build" ]
then
	build_experiment "$2" 
fi



