#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

#set -x # print commands with expanded variables

directory="eval.opengov2001months"

if [ ! -d "$directory" ]; then
  mkdir $directory
fi

# add current directory to path
export PATH="`pwd`:$PATH"

cd $directory

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

# echo "downloading files ..."
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__01/evaluation/opengov2001months__01.task.json opengov2001months__01.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__02/evaluation/opengov2001months__02.task.json opengov2001months__02.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__03/evaluation/opengov2001months__03.task.json opengov2001months__03.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__04/evaluation/opengov2001months__04.task.json opengov2001months__04.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__05/evaluation/opengov2001months__05.task.json opengov2001months__05.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__06/evaluation/opengov2001months__06.task.json opengov2001months__06.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__07/evaluation/opengov2001months__07.task.json opengov2001months__07.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__08/evaluation/opengov2001months__08.task.json opengov2001months__08.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__09/evaluation/opengov2001months__09.task.json opengov2001months__09.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__10/evaluation/opengov2001months__10.task.json opengov2001months__10.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__11/evaluation/opengov2001months__11.task.json opengov2001months__11.json
# scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/opengov2001months__12/evaluation/opengov2001months__12.task.json opengov2001months__12.json

echo "converting 1:1 files to metis format ..."
json2metis.py opengov2001months__01.json > opengov2001months__01.metis
json2metis.py opengov2001months__02.json > opengov2001months__02.metis
json2metis.py opengov2001months__03.json > opengov2001months__03.metis
json2metis.py opengov2001months__04.json > opengov2001months__04.metis
json2metis.py opengov2001months__05.json > opengov2001months__05.metis
json2metis.py opengov2001months__06.json > opengov2001months__06.metis
json2metis.py opengov2001months__07.json > opengov2001months__07.metis
json2metis.py opengov2001months__08.json > opengov2001months__08.metis
json2metis.py opengov2001months__09.json > opengov2001months__09.metis
json2metis.py opengov2001months__10.json > opengov2001months__10.metis
json2metis.py opengov2001months__11.json > opengov2001months__11.metis
json2metis.py opengov2001months__12.json > opengov2001months__12.metis

echo "partitioning 1:1 using metis ..."
gpmetis -objtype=vol opengov2001months__01.metis 12
gpmetis -objtype=vol opengov2001months__02.metis 12
gpmetis -objtype=vol opengov2001months__03.metis 12
gpmetis -objtype=vol opengov2001months__04.metis 12
gpmetis -objtype=vol opengov2001months__05.metis 12
gpmetis -objtype=vol opengov2001months__06.metis 12
gpmetis -objtype=vol opengov2001months__07.metis 12
gpmetis -objtype=vol opengov2001months__08.metis 12
gpmetis -objtype=vol opengov2001months__09.metis 12
gpmetis -objtype=vol opengov2001months__10.metis 12
gpmetis -objtype=vol opengov2001months__11.metis 12
gpmetis -objtype=vol opengov2001months__12.metis 12

echo "converting two-months files to metis format ..."
json2metis.py opengov2001months__01.json opengov2001months__02.json > opengov2001months__01-02.metis
json2metis.py opengov2001months__02.json opengov2001months__03.json > opengov2001months__02-03.metis
json2metis.py opengov2001months__03.json opengov2001months__04.json > opengov2001months__03-04.metis
json2metis.py opengov2001months__04.json opengov2001months__05.json > opengov2001months__04-05.metis
json2metis.py opengov2001months__05.json opengov2001months__06.json > opengov2001months__05-06.metis
json2metis.py opengov2001months__06.json opengov2001months__07.json > opengov2001months__06-07.metis
json2metis.py opengov2001months__07.json opengov2001months__08.json > opengov2001months__07-08.metis
json2metis.py opengov2001months__08.json opengov2001months__09.json > opengov2001months__08-09.metis
json2metis.py opengov2001months__09.json opengov2001months__10.json > opengov2001months__09-10.metis
json2metis.py opengov2001months__10.json opengov2001months__11.json > opengov2001months__10-11.metis

echo "partitioning two-months using metis ..."
gpmetis -objtype=vol opengov2001months__01-02.metis 12
gpmetis -objtype=vol opengov2001months__02-03.metis 12
gpmetis -objtype=vol opengov2001months__03-04.metis 12
gpmetis -objtype=vol opengov2001months__04-05.metis 12
gpmetis -objtype=vol opengov2001months__05-06.metis 12
gpmetis -objtype=vol opengov2001months__06-07.metis 12
gpmetis -objtype=vol opengov2001months__07-08.metis 12
gpmetis -objtype=vol opengov2001months__08-09.metis 12
gpmetis -objtype=vol opengov2001months__09-10.metis 12
gpmetis -objtype=vol opengov2001months__10-11.metis 12

echo "converting three-months files to metis format ..."
json2metis.py opengov2001months__01.json opengov2001months__02.json opengov2001months__03.json > opengov2001months__01-03.metis
json2metis.py opengov2001months__02.json opengov2001months__03.json opengov2001months__04.json > opengov2001months__02-04.metis
json2metis.py opengov2001months__03.json opengov2001months__04.json opengov2001months__05.json > opengov2001months__03-05.metis
json2metis.py opengov2001months__04.json opengov2001months__05.json opengov2001months__06.json > opengov2001months__04-06.metis
json2metis.py opengov2001months__05.json opengov2001months__06.json opengov2001months__07.json > opengov2001months__05-07.metis
json2metis.py opengov2001months__06.json opengov2001months__07.json opengov2001months__08.json > opengov2001months__06-08.metis
json2metis.py opengov2001months__07.json opengov2001months__08.json opengov2001months__09.json > opengov2001months__07-09.metis
json2metis.py opengov2001months__08.json opengov2001months__09.json opengov2001months__10.json > opengov2001months__08-10.metis
json2metis.py opengov2001months__09.json opengov2001months__10.json opengov2001months__11.json > opengov2001months__09-11.metis

echo "partitioning three-months using metis ..."
gpmetis -objtype=vol opengov2001months__01-03.metis 12
gpmetis -objtype=vol opengov2001months__02-04.metis 12
gpmetis -objtype=vol opengov2001months__03-05.metis 12
gpmetis -objtype=vol opengov2001months__04-06.metis 12
gpmetis -objtype=vol opengov2001months__05-07.metis 12
gpmetis -objtype=vol opengov2001months__06-08.metis 12
gpmetis -objtype=vol opengov2001months__07-09.metis 12
gpmetis -objtype=vol opengov2001months__08-10.metis 12
gpmetis -objtype=vol opengov2001months__09-11.metis 12



echo ""
echo "******************************"
echo "* running the evaluation 1:1 *"
echo "******************************"


for i in {1..12}
do
    padded_idx=`printf "%02d" $i`
    eval_name="opengov2001months__${padded_idx}"

    #echo "comparing distributions for $eval_name"
    compare_distributions.py --light 12 $eval_name.json $eval_name.metis.part.12
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
        compare_distributions.py --light 12 $eval_name.json $last_eval_name.metis.part.12
    fi

    last_eval_name=$eval_name
done


echo ""
echo "****************************************"
echo "* now use previous two previous months *"
echo "****************************************"

compare_distributions.py --light 12 opengov2001months__03.json opengov2001months__01-02.metis.part.12
compare_distributions.py --light 12 opengov2001months__04.json opengov2001months__02-03.metis.part.12
compare_distributions.py --light 12 opengov2001months__05.json opengov2001months__03-04.metis.part.12
compare_distributions.py --light 12 opengov2001months__06.json opengov2001months__04-05.metis.part.12
compare_distributions.py --light 12 opengov2001months__07.json opengov2001months__05-06.metis.part.12
compare_distributions.py --light 12 opengov2001months__08.json opengov2001months__06-07.metis.part.12
compare_distributions.py --light 12 opengov2001months__09.json opengov2001months__07-08.metis.part.12
compare_distributions.py --light 12 opengov2001months__10.json opengov2001months__08-09.metis.part.12
compare_distributions.py --light 12 opengov2001months__11.json opengov2001months__09-10.metis.part.12
compare_distributions.py --light 12 opengov2001months__12.json opengov2001months__10-11.metis.part.12


echo ""
echo "******************************************"
echo "* now use previous three previous months *"
echo "******************************************"

compare_distributions.py --light 12 opengov2001months__04.json opengov2001months__01-03.metis.part.12
compare_distributions.py --light 12 opengov2001months__05.json opengov2001months__02-04.metis.part.12
compare_distributions.py --light 12 opengov2001months__06.json opengov2001months__03-05.metis.part.12
compare_distributions.py --light 12 opengov2001months__07.json opengov2001months__04-06.metis.part.12
compare_distributions.py --light 12 opengov2001months__08.json opengov2001months__05-07.metis.part.12
compare_distributions.py --light 12 opengov2001months__09.json opengov2001months__06-08.metis.part.12
compare_distributions.py --light 12 opengov2001months__10.json opengov2001months__07-09.metis.part.12
compare_distributions.py --light 12 opengov2001months__11.json opengov2001months__08-10.metis.part.12
compare_distributions.py --light 12 opengov2001months__12.json opengov2001months__09-11.metis.part.12


echo ""
echo "*************************"
echo "* now load distribution *"
echo "*************************"

compare_distributions.py -o 12 opengov2001months__02.json opengov2001months__01.metis.part.12 
compare_distributions.py -o 12 opengov2001months__03.json opengov2001months__02.metis.part.12 
compare_distributions.py -o 12 opengov2001months__04.json opengov2001months__03.metis.part.12 
compare_distributions.py -o 12 opengov2001months__05.json opengov2001months__04.metis.part.12 
compare_distributions.py -o 12 opengov2001months__06.json opengov2001months__05.metis.part.12 
compare_distributions.py -o 12 opengov2001months__07.json opengov2001months__06.metis.part.12 
compare_distributions.py -o 12 opengov2001months__08.json opengov2001months__07.metis.part.12 
compare_distributions.py -o 12 opengov2001months__09.json opengov2001months__08.metis.part.12 
compare_distributions.py -o 12 opengov2001months__10.json opengov2001months__09.metis.part.12 
compare_distributions.py -o 12 opengov2001months__11.json opengov2001months__10.metis.part.12 
compare_distributions.py -o 12 opengov2001months__12.json opengov2001months__11.metis.part.12 


cd ..