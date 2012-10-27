#!/bin/bash

# This script is executed per node. It does automatically decide if it needs
# to run as the nimbus or only as the supervisor script.
# 
# Author: Thomas Hunziker
#

NODES_FOLDER="$KATTS_JOB_TMP_FOLDER/nodes"
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
cp -r "$KATTS_HOME/dependencies/storm" "$JOB_TMP_DIR/"

#JAVA_HOME="/usr/lib/jvm/java-6-sun-1.6.0.26"


EVALUATION_FOLDER="$KATTS_JOB_FOLDER/evaluation"
rm -rdf "$EVALUATION_FOLDER"
if [ ! -d "$EVALUATION_FOLDER" ]; then
	mkdir "$EVALUATION_FOLDER"
fi

KATTS_JOB_FOLDER="$KATTS_JOB_FOLDER"
KATTS_JOB="$KATTS_JOB"

# Copy data from home to temp direcotry:
cp -r "$KATTS_JOB_FOLDER/data" "$JOB_TMP_DIR/"


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
	
	ZOO_LOG_DIR="$KATTS_JOB_TMP_FOLDER/zookeeper-log"
	
	# Create Tmp folder
	if [ ! -d "$ZOO_LOG_DIR" ]; then
		mkdir "$ZOO_LOG_DIR"
	fi
	
	export ZOO_LOG_DIR
	
	# Start ZooKeeper Server
	bash "$JOB_TMP_DIR/zookeeper/bin/zkServer.sh" start
	
	# Debug output
	echo "Start the nimbus daemon"
	
	# Start Nimbus node 
	NIMBUS_JOB="$JOB_TMP_DIR/storm/bin/storm nimbus"
	eval ${NIMBUS_JOB} &
	NIMBUS_JOB_PID=`ps ax | grep -e "${NIMBUS_JOB}" | grep -v grep | awk '{print $1}'`

	# Set the processor affinity (this works only with linux like systems in this way)
	taskset -cp "$PROCESSOR_LIST" "$NIMBUS_JOB_PID"


	# Start UI
	if [ "$STARTUP_UI" == "yes" ]
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
echo "Start the supervisor daemon"


# Startup the supervisor
SUPERVISOR_JOB="$JOB_TMP_DIR/storm/bin/storm supervisor"
eval ${SUPERVISOR_JOB} &
SUPERVISOR_JOB_PID=`ps ax | grep -e "${SUPERVISOR_JOB}" | grep -v grep | awk '{print $1}'`

# Set the processor affinity (this works only with linux like systems in this way)
taskset -cp "$PROCESSOR_LIST" "$SUPERVISOR_JOB_PID"


# Wait that the supervisor is ready
sleep 10

TERMINATION_FILE="$EVALUATION_FOLDER/terminated_on"

MONITORING_DATA_PATH="$JOB_TMP_DIR/evaluation_data"
if [ ! -d "$MONITORING_DATA_PATH" ]; then
	mkdir "$MONITORING_DATA_PATH"
fi

if [ "$nimbusNode" == "$nodeHostname" ]; then
	
	# Variable that are exportet to the $ADDITIONAL_PARAMETERS and to the query.xml
	
	variables="JOB_TMP_DIR EVALUATION_FOLDER MONITORING_DATA_PATH KATTS_JOB_FOLDER TERMINATION_FILE KATTS_JOB"

	for variable in $variables
	do
		variableName="value=\$$variable"
		eval $variableName
	
		ADDITIONAL_PARAMETERS=$(echo "$ADDITIONAL_PARAMETERS" | sed "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g")
	done

	# Create the query script:
	QUERY_TMP_FILE="$KATTS_JOB_TMP_FOLDER/query.xml"
	cp "$QUERY_FILE" "$QUERY_TMP_FILE"

	for variable in $variables
	do
		variableName="value=\$$variable"
		eval $variableName
	
		sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$QUERY_TMP_FILE"
	done


	# Debug output
	echo ""
	echo "Deploy query"
	
	chmod -R 0777 "$JOB_TMP_DIR"
	chmod -R 0777 "$KATTS_JOB_FOLDER"
	
	# Deploy the topology to the storm cluster	
	DEPLOY_TOPOLOGY="$JOB_TMP_DIR/storm/bin/storm jar $TOPOLOGY_JAR $TOPOLOGY_DEPLOYMENT_CLASS_NAME --topology-name $KATTS_JOB --number-of-processors $NUMBER_OF_PROCESSORS --number-of-workers $NUMBER_OF_NODES  $ADDITIONAL_PARAMETERS $QUERY_TMP_FILE -c nimbus.host=$nimbusNode"
	eval ${DEPLOY_TOPOLOGY}
	
	# Write the number of nodes for this job into the evaluation folder:
	echo "$NUMBER_OF_NODES" > "$EVALUATION_FOLDER/number_of_nodes"
	
	# Write the number of processors per node for this job into the evaluation folder:
	echo "$NUMBER_OF_PROCESSORS_PER_NODE" > "$EVALUATION_FOLDER/number_of_processors_per_node"
	
	
fi

chmod -R 0777 "$JOB_TMP_DIR"

# Debug output
echo "Running... "


# Monitor the termination file, if it is written, then we can continue
while [ ! -f "$TERMINATION_FILE" ]; do
	sleep 15
done

# Debug output
echo "Terminating the processes... "


if [ "$nimbusNode" == "$nodeHostname" ]; then
	
	DEPLOY_TOPOLOGY="$JOB_TMP_DIR/storm/bin/storm kill $KATTS_JOB -c nimbus.host=$nimbusNode"
	eval ${DEPLOY_TOPOLOGY}
	
	kill $NIMBUS_JOB_PID
	
	# Kill UI
	if [ "$STARTUP_UI" == "yes" ]
	then
		kill $UI_JOB_PID
	fi
	
	# Create the data dir
	mkdir "$EVALUATION_FOLDER/data"
	
else 
	# Give the nimbus time to shutdown the toplogy, before killing the supervisors
	sleep 16
fi


kill $SUPERVISOR_JOB_PID

if [ "$nimbusNode" == "$nodeHostname" ]; then
	
	COMPLEATION_FILE="$EVALUATION_FOLDER/completed_on"
	touch "$COMPLEATION_FILE"
fi
