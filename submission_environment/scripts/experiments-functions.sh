#!/bin/bash


# This function runs the experiments. This means all the jobs are submitted to the cluster.
function run_experiment() {
	
	experiment_name="$1"
	
	# Find all jobs that starts with the experiment name and followed by two underlines
	katts_jobs="$KATTS_JOB_FOLDER/$experiment_name"__*
	
	for katts_job in $katts_jobs
	do
		jobName=$(basename "$katts_job")
		jobCommand="$KATTS_HOME/job.sh $jobName"
		eval ${jobCommand}
	done
}


function build_experiment() {

	if [ ! -f "$KATTS_EXPERIMENT_FOLDER/build.sh" ]
	then
		echo "The given experiment has no build.sh script."
		exit;
	fi
	
	EXPERIMENT_NAME="$1"
	
	# Remove all old jobs:
	katts_jobs="$KATTS_JOB_FOLDER/$EXPERIMENT_NAME"__*
	
	for katts_job in $katts_jobs
	do
		rm -rd "$katts_job"
	done 
	
	. "$KATTS_EXPERIMENT_FOLDER/build.sh"
}


function create_job() {
	
	job_name_prefix="$1"
	job_template_directory="$2"
	variables="$3"
	
	jobName="$EXPERIMENT_NAME"__"$job_name_prefix"
	
	#echo "${variables["PROCESSORS"]}"
	
	jobFolder="$KATTS_JOB_FOLDER/$jobName"
	mkdir $jobFolder
	cp -r $job_template_directory/* $jobFolder
	
	# Apply the variables on the bootstrap.sh and the query.xml
	for variable in "${!variables[@]}"; 
	do 
		if [ ! "$variable" == "0" ]
		then
			value="${variables["$variable"]}"
			
			bootstrapFile="$jobFolder/bootstrap.sh"
			if [ -f $bootstrapFile ]; 
			then
				sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$bootstrapFile"
			fi
			
			queryFile="$jobFolder/query.xml"
			if [ -f $queryFile ]; 
			then
				sed -i "s/\$$variable/$(echo $value | sed -e 's/\\/\\\\/g' -e 's/\//\\\//g' -e 's/&/\\\&/g')/g" "$queryFile"
			fi
		fi
	done
}


function usage() {
	echo 'Usage: experiment.sh [command] experiment_name';
	echo "command: 'build' or 'run'"
	echo "experiment_name: The folder name in ./experiments";
	echo ""
	echo "E.g. ./experiment.sh build sample-experiment"
	echo ""
	echo "Important: Before you can run the experiment you need to build it!"
	exit;
}


