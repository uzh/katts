#!/bin/bash


ADDITIONAL_PARAMETERS='--monitoring 1 --monitoring-record-interval 15 --termination-file-path $TERMINATION_FILE --monitoring-path $MONITORING_DATA_PATH --topology-name $EXPERIMENT'
TOPOLOGY_DEPLOYMENT_CLASS_NAME="ch.uzh.ddis.katts.RunXmlQuery"
NUMBER_OF_NODES="2"
WALLTIME="00:60:00"

