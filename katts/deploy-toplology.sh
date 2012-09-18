#!/bin/bash


log_dir="`pwd`/out-torque"
cat torque_script.sh | qsub -d $log_dir
