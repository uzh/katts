#!/bin/bash


ADDITIONAL_PARAMETERS='--monitoring 1 --monitoring-record-interval 15 --termination-file-path $TERMINATION_FILE --monitoring-path $MONITORING_DATA_PATH --topology-name $EXPERIMENT --factor-of-threads-per-processor 2.2'
TOPOLOGY_DEPLOYMENT_CLASS_NAME="ch.uzh.ddis.katts.RunXmlQuery"
NUMBER_OF_NODES="3"
NODE_PROCESSOR_LIST=$(seq 0 22) # Define a list of processor numbers.
STARTUP_UI="yes" # or "no"
WALLTIME="00:60:00"
NUMBER_OF_PROCESSOR_RESERVED_PER_NODE="23" # This value should be the number of processors that each machine has, or the experiment may be corruped.
EXPECTED_MEMORY_CONSUMPTION="55000mb"

