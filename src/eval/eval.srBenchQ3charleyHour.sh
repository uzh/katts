#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

#set -x # print commands with expanded variables

directory="eval.srBenchQ3charleyHour"

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

echo "downloading files ..."
# for test in 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23
# do
#     echo "scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__$test/evaluation/srBenchQ3charleyHour__$test.task.json srBenchQ3charleyHour__$test.json"
# done
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__00/evaluation/srBenchQ3charleyHour__00.task.json srBenchQ3charleyHour__00.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__01/evaluation/srBenchQ3charleyHour__01.task.json srBenchQ3charleyHour__01.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__02/evaluation/srBenchQ3charleyHour__02.task.json srBenchQ3charleyHour__02.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__03/evaluation/srBenchQ3charleyHour__03.task.json srBenchQ3charleyHour__03.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__04/evaluation/srBenchQ3charleyHour__04.task.json srBenchQ3charleyHour__04.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__05/evaluation/srBenchQ3charleyHour__05.task.json srBenchQ3charleyHour__05.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__06/evaluation/srBenchQ3charleyHour__06.task.json srBenchQ3charleyHour__06.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__07/evaluation/srBenchQ3charleyHour__07.task.json srBenchQ3charleyHour__07.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__08/evaluation/srBenchQ3charleyHour__08.task.json srBenchQ3charleyHour__08.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__09/evaluation/srBenchQ3charleyHour__09.task.json srBenchQ3charleyHour__09.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__10/evaluation/srBenchQ3charleyHour__10.task.json srBenchQ3charleyHour__10.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__11/evaluation/srBenchQ3charleyHour__11.task.json srBenchQ3charleyHour__11.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__12/evaluation/srBenchQ3charleyHour__12.task.json srBenchQ3charleyHour__12.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__13/evaluation/srBenchQ3charleyHour__13.task.json srBenchQ3charleyHour__13.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__14/evaluation/srBenchQ3charleyHour__14.task.json srBenchQ3charleyHour__14.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__15/evaluation/srBenchQ3charleyHour__15.task.json srBenchQ3charleyHour__15.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__16/evaluation/srBenchQ3charleyHour__16.task.json srBenchQ3charleyHour__16.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__17/evaluation/srBenchQ3charleyHour__17.task.json srBenchQ3charleyHour__17.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__18/evaluation/srBenchQ3charleyHour__18.task.json srBenchQ3charleyHour__18.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__19/evaluation/srBenchQ3charleyHour__19.task.json srBenchQ3charleyHour__19.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__20/evaluation/srBenchQ3charleyHour__20.task.json srBenchQ3charleyHour__20.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__21/evaluation/srBenchQ3charleyHour__21.task.json srBenchQ3charleyHour__21.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__22/evaluation/srBenchQ3charleyHour__22.task.json srBenchQ3charleyHour__22.json
scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charleyHour__23/evaluation/srBenchQ3charleyHour__23.task.json srBenchQ3charleyHour__23.json

echo "converting 1:1 files to metis format ..."
json2metis.py srBenchQ3charleyHour__00.json > srBenchQ3charleyHour__00.metis
json2metis.py srBenchQ3charleyHour__01.json > srBenchQ3charleyHour__01.metis
json2metis.py srBenchQ3charleyHour__02.json > srBenchQ3charleyHour__02.metis
json2metis.py srBenchQ3charleyHour__03.json > srBenchQ3charleyHour__03.metis
json2metis.py srBenchQ3charleyHour__04.json > srBenchQ3charleyHour__04.metis
json2metis.py srBenchQ3charleyHour__05.json > srBenchQ3charleyHour__05.metis
json2metis.py srBenchQ3charleyHour__06.json > srBenchQ3charleyHour__06.metis
json2metis.py srBenchQ3charleyHour__07.json > srBenchQ3charleyHour__07.metis
json2metis.py srBenchQ3charleyHour__08.json > srBenchQ3charleyHour__08.metis
json2metis.py srBenchQ3charleyHour__09.json > srBenchQ3charleyHour__09.metis
json2metis.py srBenchQ3charleyHour__10.json > srBenchQ3charleyHour__10.metis
json2metis.py srBenchQ3charleyHour__11.json > srBenchQ3charleyHour__11.metis
json2metis.py srBenchQ3charleyHour__12.json > srBenchQ3charleyHour__12.metis
json2metis.py srBenchQ3charleyHour__13.json > srBenchQ3charleyHour__13.metis
json2metis.py srBenchQ3charleyHour__14.json > srBenchQ3charleyHour__14.metis
json2metis.py srBenchQ3charleyHour__15.json > srBenchQ3charleyHour__15.metis
json2metis.py srBenchQ3charleyHour__16.json > srBenchQ3charleyHour__16.metis
json2metis.py srBenchQ3charleyHour__17.json > srBenchQ3charleyHour__17.metis
json2metis.py srBenchQ3charleyHour__18.json > srBenchQ3charleyHour__18.metis
json2metis.py srBenchQ3charleyHour__19.json > srBenchQ3charleyHour__19.metis
json2metis.py srBenchQ3charleyHour__20.json > srBenchQ3charleyHour__20.metis
json2metis.py srBenchQ3charleyHour__21.json > srBenchQ3charleyHour__21.metis
json2metis.py srBenchQ3charleyHour__22.json > srBenchQ3charleyHour__22.metis
json2metis.py srBenchQ3charleyHour__23.json > srBenchQ3charleyHour__23.metis


echo "partitioning 1:1 using metis ..."
gpmetis -objtype=vol srBenchQ3charleyHour__00.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__01.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__02.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__03.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__04.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__05.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__06.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__07.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__08.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__09.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__10.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__11.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__12.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__13.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__14.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__15.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__16.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__17.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__18.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__19.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__20.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__21.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__22.metis 12
gpmetis -objtype=vol srBenchQ3charleyHour__23.metis 12


# echo "converting two intervals into one metis file ..."
# json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json > srBenchQ3charley5minute__00-10.metis
# json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json > srBenchQ3charley5minute__05-15.metis
# json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__10-20.metis
# json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__15-25.metis
# json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__20-30.metis
# json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__25-35.metis
# json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__30-40.metis
# json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__35-45.metis
# json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__40-50.metis
# json2metis.py srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__45-55.metis
# json2metis.py srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__50-60.metis

# echo "partitioning two-interval files using metis ..."
# gpmetis -objtype=vol srBenchQ3charley5minute__00-10.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__05-15.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__10-20.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__15-25.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__20-30.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__25-35.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__30-40.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__35-45.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__40-50.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__45-55.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__50-60.metis 12

# echo "converting three intervals into one metis file ..."
# json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json > srBenchQ3charley5minute__00-15.metis
# json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__05-20.metis
# json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__10-25.metis
# json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__15-30.metis
# json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__20-35.metis
# json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__25-40.metis
# json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__30-45.metis
# json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__35-50.metis
# json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__40-55.metis
# json2metis.py srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__45-60.metis

# echo "partitioning three-interval files using metis ..."
# gpmetis -objtype=vol srBenchQ3charley5minute__00-15.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__05-20.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__10-25.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__15-30.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__20-35.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__25-40.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__30-45.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__35-50.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__40-55.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__45-60.metis 12

# echo "converting four intervals into one metis file ..."
# json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__00-20.metis
# json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__05-25.metis
# json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__10-30.metis
# json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__15-35.metis
# json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__20-40.metis
# json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__25-45.metis
# json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__30-50.metis
# json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__35-55.metis
# json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__40-60.metis

# echo "partitioning four-interval files using metis ..."
# gpmetis -objtype=vol srBenchQ3charley5minute__00-20.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__05-25.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__10-30.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__15-35.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__20-40.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__25-45.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__30-50.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__35-55.metis 12
# gpmetis -objtype=vol srBenchQ3charley5minute__40-60.metis 12


echo ""
echo "******************************"
echo "* running the evaluation 1:1 *"
echo "******************************"

compare_distributions.py --light 12 srBenchQ3charleyHour__00.json srBenchQ3charleyHour__00.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__01.json srBenchQ3charleyHour__01.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__02.json srBenchQ3charleyHour__02.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__03.json srBenchQ3charleyHour__03.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__04.json srBenchQ3charleyHour__04.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__05.json srBenchQ3charleyHour__05.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__06.json srBenchQ3charleyHour__06.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__07.json srBenchQ3charleyHour__07.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__08.json srBenchQ3charleyHour__08.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__09.json srBenchQ3charleyHour__09.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__10.json srBenchQ3charleyHour__10.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__11.json srBenchQ3charleyHour__11.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__12.json srBenchQ3charleyHour__12.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__13.json srBenchQ3charleyHour__13.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__14.json srBenchQ3charleyHour__14.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__15.json srBenchQ3charleyHour__15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__16.json srBenchQ3charleyHour__16.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__17.json srBenchQ3charleyHour__17.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__18.json srBenchQ3charleyHour__18.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__19.json srBenchQ3charleyHour__19.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__20.json srBenchQ3charleyHour__20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__21.json srBenchQ3charleyHour__21.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__22.json srBenchQ3charleyHour__22.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__23.json srBenchQ3charleyHour__23.metis.part.12



echo ""
echo "*********************************************"
echo "* now use previous interval to compare with *"
echo "*********************************************"

compare_distributions.py --light 12 srBenchQ3charleyHour__01.json srBenchQ3charleyHour__00.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__02.json srBenchQ3charleyHour__01.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__03.json srBenchQ3charleyHour__02.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__04.json srBenchQ3charleyHour__03.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__05.json srBenchQ3charleyHour__04.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__06.json srBenchQ3charleyHour__05.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__07.json srBenchQ3charleyHour__06.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__08.json srBenchQ3charleyHour__07.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__09.json srBenchQ3charleyHour__08.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__10.json srBenchQ3charleyHour__09.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__11.json srBenchQ3charleyHour__10.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__12.json srBenchQ3charleyHour__11.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__13.json srBenchQ3charleyHour__12.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__14.json srBenchQ3charleyHour__13.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__15.json srBenchQ3charleyHour__14.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__16.json srBenchQ3charleyHour__15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__17.json srBenchQ3charleyHour__16.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__18.json srBenchQ3charleyHour__17.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__19.json srBenchQ3charleyHour__18.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__20.json srBenchQ3charleyHour__19.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__21.json srBenchQ3charleyHour__20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__22.json srBenchQ3charleyHour__21.metis.part.12
compare_distributions.py --light 12 srBenchQ3charleyHour__23.json srBenchQ3charleyHour__22.metis.part.12


# echo ""
# echo "**********************************"
# echo "* now use previous two intervals *"
# echo "**********************************"

# compare_distributions.py --light 12 srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__00-10.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__05-15.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__10-20.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__15-25.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__20-30.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__25-35.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__30-40.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__35-45.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__40-50.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__45-55.metis.part.12


# echo ""
# echo "******************************************"
# echo "* now use previous three previous months *"
# echo "******************************************"

# compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__00-15.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__05-20.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__10-25.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__15-30.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__20-35.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__25-40.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__30-45.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__35-50.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__40-55.metis.part.12


# echo ""
# echo "******************************************"
# echo "* now use previous four previous months *"
# echo "******************************************"

# compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__00-20.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__05-25.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__10-30.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__15-35.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__20-40.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__25-45.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__30-50.metis.part.12
# compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__35-55.metis.part.12


cd ..