#!/bin/bash


nodeHostname=$(hostname)
JOB_TMP_DIR="/home/torque/tmp/$PBS_O_LOGNAME.$PBS_JOBID"
MONITORING_DATA_PATH="$JOB_TMP_DIR/evaluation_data"
EVALUATION_FOLDER="$KATTS_JOB_FOLDER/evaluation"


# Copy all evaluation data to the katts_job folder
EVALUATION_DATA_FOLDER="$EVALUATION_FOLDER/data/$nodeHostname"
mkdir "$EVALUATION_DATA_FOLDER"

cp -r "$MONITORING_DATA_PATH"/* "$EVALUATION_DATA_FOLDER"

