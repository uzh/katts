#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

#set -x # print commands with expanded variables

directory="eval.srBenchQ3charleyDay"

if [ ! -d "$directory" ]; then
  mkdir $directory
fi

# add current directory to path
export PATH="`pwd`:$PATH"

cd $directory

echo "\n\n"
echo "******************************"
echo "* downloading and converting *"
echo "******************************"

# echo "downloading files ..."
# for test in 08 09 10 11 12 13
# do
#     scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyDay__$test/evaluation/srBenchQ3charleyDay__$test.task.json srBenchQ3charleyDay__$test.json
# done


echo "converting 1:1 files to metis format ..."
for test in 08 09 10 11 12 13
do
    json2metis.py srBenchQ3charleyDay__$test.json > srBenchQ3charleyDay__$test.metis
done


echo "partitioning 1:1 using metis ..."
for test in 08 09 10 11 12 13
do
    gpmetis -objtype=vol srBenchQ3charleyDay__$test.metis 12
done

echo ""
echo "******************************"
echo "* running the evaluation 1:1 *"
echo "******************************"

for test in 08 09 10 11 12 13
do
    compare_distributions.py --light 12 srBenchQ3charleyDay__$test.json srBenchQ3charleyDay__$test.metis.part.12
done


echo ""
echo "*********************************************"
echo "* now use previous interval to compare with *"
echo "*********************************************"

for test in 08 09 10 11 12 13
do
    if [ $test != "08" ]
    then
        compare_distributions.py --light 12 srBenchQ3charleyDay__$test.json srBenchQ3charleyDay__$last_test.metis.part.12
    fi
    last_test=$test
done

echo ""
echo "*************************"
echo "* now load distribution *"
echo "*************************"

compare_distributions.py -o 12 srBenchQ3charleyDay__09.json srBenchQ3charleyDay__08.metis.part.12 
compare_distributions.py -o 12 srBenchQ3charleyDay__10.json srBenchQ3charleyDay__09.metis.part.12 
compare_distributions.py -o 12 srBenchQ3charleyDay__11.json srBenchQ3charleyDay__10.metis.part.12 
compare_distributions.py -o 12 srBenchQ3charleyDay__12.json srBenchQ3charleyDay__11.metis.part.12 
compare_distributions.py -o 12 srBenchQ3charleyDay__13.json srBenchQ3charleyDay__12.metis.part.12 

cd ..