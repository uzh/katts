#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

set -x # print commands with expanded variables
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/$1/evaluation/$1.task.json $1.json
./do_all.sh $1