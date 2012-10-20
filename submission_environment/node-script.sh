#!/bin/bash

# This script is executed per node. It does automatically decide if it needs
# to run as the nimbus or only as the supervisor script.
# 
# Author: Thomas Hunziker
#

NODES_FOLDER="$EXPERIMENT_TMP_FOLDER/nodes"
if [ ! -d "$NODES_FOLDER" ]; then
	mkdir "$NODES_FOLDER"
fi

nodeHostname=$(hostname)
touch "$NODES_FOLDER/$nodeHostname"


JOB_TMP_DIR="/home/torque/tmp/$PBS_O_LOGNAME.$PBS_JOBID"

chmod -R 0777 "$JOB_TMP_DIR"

# Sleep 3 seconds to give other nodes to write the hostname
sleep 3


# Get sorted node list
nodes=`ls -1 "$NODES_FOLDER" | sort`

# Define the Nimbus node
nimbusNode=`echo $nodes | awk '{print $1}'`

# Move the storm library to the job tmp folder, since we need per node
# configurations and we need a way to prevent storm of writting to the same
# log direcotry. 
cp -r "$KATTS_HOME/storm" "$JOB_TMP_DIR/"

#JAVA_HOME="/usr/lib/jvm/java-6-sun-1.6.0.26"


EVALUATION_FOLDER="$EXPERIMENT_FOLDER/evaluation"
rm -rdf "$EVALUATION_FOLDER"
if [ ! -d "$EVALUATION_FOLDER" ]; then
	mkdir "$EVALUATION_FOLDER"
fi

EXPERIMENT_FOLDER="$EXPERIMENT_FOLDER"
EXPERIMENT="$EXPERIMENT"



# Construct the processor affinity list:
PROCESSOR_LIST=""
NUMBER_OF_PROCESSORS_PER_NODE=0
for processorId in $NODE_PROCESSOR_LIST
do
	if [ "$PROCESSOR_LIST" == "" ]; then
		PROCESSOR_LIST="$processorId"
	else
		PROCESSOR_LIST="$PROCESSOR_LIST,$processorId"
	fi
	let NUMBER_OF_PROCESSORS_PER_NODE=NUMBER_OF_PROCESSORS_PER_NODE+1 
done

# Ensure that the number of nodes variable is set correctly
NUMBER_OF_NODES="$NUMBER_OF_NODES"

let NUMBER_OF_PROCESSORS=NUMBER_OF_PROCESSORS_PER_NODE*NUMBER_OF_NODES


# Setup strom configuration
kattsHome="$KATTS_HOME"
sed -i "s/\$JOB_TMP_DIR/$(echo $JOB_TMP_DIR | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$JOB_TMP_DIR/storm/conf/storm.yaml"
sed -i "s/\$NIMBUS_HOSTNAME/$(echo $nimbusNode | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$JOB_TMP_DIR/storm/conf/storm.yaml"
sed -i "s/\$LOCAL_HOSTNAME/$(echo $nodeHostname | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$JOB_TMP_DIR/storm/conf/storm.yaml"
sed -i "s/\$KATTS_DIR/$(echo $kattsHome | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$JOB_TMP_DIR/storm/conf/storm.yaml"


if [ "$nimbusNode" == "$nodeHostname" ]; then
	# We need per Job configurations / logs, so we need to copy the binaries
	cp -r "$KATTS_HOME/dependencies/zookeeper" "$JOB_TMP_DIR/"
	
	# Change the ZooKeeper Configurations
	ZOOKEEPER_FOLDER="$JOB_TMP_DIR/zookeeper"
	sed -i "s/\$JOB_TMP_DIR/$(echo $JOB_TMP_DIR | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$ZOOKEEPER_FOLDER/conf/zoo.cfg"
	
	ZOO_LOG_DIR="$EXPERIMENT_TMP_FOLDER/zookeeper-log"
	
	# Create Tmp folder
	if [ ! -d "$ZOO_LOG_DIR" ]; then
		mkdir "$ZOO_LOG_DIR"
	fi
	
	export ZOO_LOG_DIR
	
	# Start ZooKeeper Server
	bash "$JOB_TMP_DIR/zookeeper/bin/zkServer.sh" start
	
	# Debug output
	if [ "$DEBUG" == "True" ]
	then
		echo "Start the nimbus daemon"
	fi
	
	# Start Nimbus node 
	NIMBUS_JOB="$JOB_TMP_DIR/storm/bin/storm nimbus"
	eval ${NIMBUS_JOB} &
	NIMBUS_JOB_PID=`ps ax | grep -e "${NIMBUS_JOB}" | grep -v grep | awk '{print $1}'`

	# Set the processor affinity (this works only with linux like systems in this way)
	taskset -cp "$PROCESSOR_LIST" "$NIMBUS_JOB_PID"


	# Start UI
	if [ "$NUMBER_OF_PROCESSES_PER_NODE" == "yes" ]
	then
		UI_JOB="$JOB_TMP_DIR/storm/bin/storm ui"
		eval ${UI_JOB} &
		UI_JOB_PID=`ps ax | grep -e "${UI_JOB}" | grep -v grep | awk '{print $1}'`
	fi



else
	# Since we need to start ZooKeeper on the master node, we need to wait a bit
	# to give time to setup the cluster, before starting the supervisors
	sleep 20
fi


# Debug output
if [ "$DEBUG" == "True" ]
then
	echo "Start the supervisor daemon"
fi


# Startup the supervisor
SUPERVISOR_JOB="$JOB_TMP_DIR/storm/bin/storm supervisor"
eval ${SUPERVISOR_JOB} &
SUPERVISOR_JOB_PID=`ps ax | grep -e "${SUPERVISOR_JOB}" | grep -v grep | awk '{print $1}'`

# Set the processor affinity (this works only with linux like systems in this way)
taskset -cp "$PROCESSOR_LIST" "$SUPERVISOR_JOB_PID"


# Wait that the supervisor is ready
sleep 20

TERMINATION_FILE="$EVALUATION_FOLDER/terminated_on"

MONITORING_DATA_PATH="$JOB_TMP_DIR/evaluation_data"
if [ ! -d "$MONITORING_DATA_PATH" ]; then
	mkdir "$MONITORING_DATA_PATH"
fi

if [ "$nimbusNode" == "$nodeHostname" ]; then
	
	# Variable that are exportet to the $ADDITIONAL_PARAMETERS and to the query.xml
	
	variables="JOB_TMP_DIR EVALUATION_FOLDER MONITORING_DATA_PATH EXPERIMENT_FOLDER TERMINATION_FILE EXPERIMENT"

	for variable in $variables
	do
		variableName="value=\$$variable"
		eval $variableName
	
		ADDITIONAL_PARAMETERS=$(echo "$ADDITIONAL_PARAMETERS" | sed "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g")
	done

	# Create the query script:
	QUERY_TMP_FILE="$EXPERIMENT_TMP_FOLDER/query.xml"
	cp "$QUERY_FILE" "$QUERY_TMP_FILE"

	for variable in $variables
	do
		variableName="value=\$$variable"
		eval $variableName
	
		sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$QUERY_TMP_FILE"
	done


	# Debug output
	if [ "$DEBUG" == "True" ]
	then
		echo ""
		echo "Deploy query"
	fi
	
	chmod -R 0777 "$JOB_TMP_DIR"
	chmod -R 0777 "$EXPERIMENT_FOLDER"
	
	# Deploy the topology to the storm cluster	
	DEPLOY_TOPOLOGY="$JOB_TMP_DIR/storm/bin/storm jar $TOPOLOGY_JAR $TOPOLOGY_DEPLOYMENT_CLASS_NAME --number-of-processors $NUMBER_OF_PROCESSORS --number-of-workers $NUMBER_OF_NODES  $ADDITIONAL_PARAMETERS $QUERY_TMP_FILE -c nimbus.host=$nimbusNode"
	eval ${DEPLOY_TOPOLOGY}
	
	# Make a file to store the starting time
	touch "$EVALUATION_FOLDER/start"
fi

chmod -R 0777 "$JOB_TMP_DIR"

# Debug output
if [ "$DEBUG" == "True" ]
then
	echo "Running... "
fi


# Monitor the termination file, if it is written, then we can continue
while [ ! -f "$TERMINATION_FILE" ]; do
	sleep 15
done

# Debug output
if [ "$DEBUG" == "True" ]
then
	echo "Terminating the processes... "
fi


# Kill the processes
kill $SUPERVISOR_JOB_PID

if [ "$nimbusNode" == "$nodeHostname" ]; then
	kill $NIMBUS_JOB_PID
	
	# Kill UI
	if [ "$NUMBER_OF_PROCESSES_PER_NODE" == "yes" ]
	then
		kill $UI_JOB_PID
	fi
fi

# Give the processes sometime to shutdown
sleep 10

# Copy all evaluation data to the experiment folder
mkdir "$EVALUATION_FOLDER/data"
EVALUATION_DATA_FOLDER="$EVALUATION_FOLDER/data/$nodeHostname"
mkdir "$EVALUATION_DATA_FOLDER"
cp -r "$MONITORING_DATA_PATH"/* "$EVALUATION_DATA_FOLDER"

if [ "$nimbusNode" == "$nodeHostname" ]; then
	COMPLEATION_FILE="$EVALUATION_FOLDER/completed_on"
	touch "$COMPLEATION_FILE"
fi
