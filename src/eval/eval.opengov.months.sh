#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

#set -x # print commands with expanded variables

directory="eval.opengov.months"

if [ ! -d "$directory" ]; then
  mkdir $directory
fi


# echo "\n\n"
# echo "******************************"
# echo "* downloading and converting *"
# echo "******************************"

# for i in {1..12}
# do
#     padded_idx=`printf "%02d" $i`
#     eval_name="opengov2001months__${padded_idx}"

#     echo "downloading $eval_dir"
#     scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/$eval_name/evaluation/$eval_name.task.json $directory/$eval_name.json

#     echo "converting to metis input file"
#     python json2metis.py $directory/$eval_name.json > $directory/$eval_name.metis

#     echo "partitioning using metis"
#     gpmetis -objtype=vol $directory/$eval_name.metis 12
# done


echo ""
echo "******************************"
echo "* running the evaluation 1:1 *"
echo "******************************"


for i in {1..12}
do
    padded_idx=`printf "%02d" $i`
    eval_name="opengov2001months__${padded_idx}"

    #echo "comparing distributions for $eval_name"
    python compare_distributions.py --light 12 $directory/$eval_name.json $directory/$eval_name.metis.part.12
done


echo ""
echo "******************************************"
echo "* now use previous month to compare with *"
echo "******************************************"

last_eval_name=""
for i in {1..12}
do
    padded_idx=`printf "%02d" $i`
    eval_name="opengov2001months__${padded_idx}"

    if [ $i -gt 1 ]
    then
        #echo "comparing distributions of $eval_name with partitioning of $last_eval_name"
        python compare_distributions.py --light 12 $directory/$eval_name.json $directory/$last_eval_name.metis.part.12
    fi

    last_eval_name=$eval_name
done

