#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

#set -x # print commands with expanded variables

directory="eval.srBenchQ3charley5minute"

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
for test in 00-05 05-10 10-15 15-20 20-25 25-30 30-35 35-40 40-45 45-50 50-55 55-60
do
    scp lfischer@kraken.ifi.uzh.ch:~/katts_submission_environment/katts_jobs/srBenchQ3charley5minute__$test/evaluation/srBenchQ3charley5minute__$test.task.json srBenchQ3charley5minute__$test.json
done

echo "converting 1:1 files to metis format ..."
json2metis.py srBenchQ3charley5minute__00-05.json > srBenchQ3charley5minute__00-05.metis
json2metis.py srBenchQ3charley5minute__05-10.json > srBenchQ3charley5minute__05-10.metis
json2metis.py srBenchQ3charley5minute__10-15.json > srBenchQ3charley5minute__10-15.metis
json2metis.py srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__15-20.metis
json2metis.py srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__20-25.metis
json2metis.py srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__25-30.metis
json2metis.py srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__30-35.metis
json2metis.py srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__35-40.metis
json2metis.py srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__40-45.metis
json2metis.py srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__45-50.metis
json2metis.py srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__50-55.metis
json2metis.py srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__55-60.metis

echo "partitioning 1:1 using metis ..."
gpmetis -objtype=vol srBenchQ3charley5minute__00-05.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__05-10.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__10-15.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__15-20.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__20-25.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__25-30.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__30-35.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__35-40.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__40-45.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__45-50.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__50-55.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__55-60.metis 12

echo "converting two intervals into one metis file ..."
json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json > srBenchQ3charley5minute__00-10.metis
json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json > srBenchQ3charley5minute__05-15.metis
json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__10-20.metis
json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__15-25.metis
json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__20-30.metis
json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__25-35.metis
json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__30-40.metis
json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__35-45.metis
json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__40-50.metis
json2metis.py srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__45-55.metis
json2metis.py srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__50-60.metis

echo "partitioning two-interval files using metis ..."
gpmetis -objtype=vol srBenchQ3charley5minute__00-10.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__05-15.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__10-20.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__15-25.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__20-30.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__25-35.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__30-40.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__35-45.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__40-50.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__45-55.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__50-60.metis 12

echo "converting three intervals into one metis file ..."
json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json > srBenchQ3charley5minute__00-15.metis
json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__05-20.metis
json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__10-25.metis
json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__15-30.metis
json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__20-35.metis
json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__25-40.metis
json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__30-45.metis
json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__35-50.metis
json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__40-55.metis
json2metis.py srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__45-60.metis

echo "partitioning three-interval files using metis ..."
gpmetis -objtype=vol srBenchQ3charley5minute__00-15.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__05-20.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__10-25.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__15-30.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__20-35.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__25-40.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__30-45.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__35-50.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__40-55.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__45-60.metis 12

echo "converting four intervals into one metis file ..."
json2metis.py srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json > srBenchQ3charley5minute__00-20.metis
json2metis.py srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json > srBenchQ3charley5minute__05-25.metis
json2metis.py srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json > srBenchQ3charley5minute__10-30.metis
json2metis.py srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json > srBenchQ3charley5minute__15-35.metis
json2metis.py srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json > srBenchQ3charley5minute__20-40.metis
json2metis.py srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json > srBenchQ3charley5minute__25-45.metis
json2metis.py srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json > srBenchQ3charley5minute__30-50.metis
json2metis.py srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json > srBenchQ3charley5minute__35-55.metis
json2metis.py srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__55-60.json > srBenchQ3charley5minute__40-60.metis

echo "partitioning four-interval files using metis ..."
gpmetis -objtype=vol srBenchQ3charley5minute__00-20.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__05-25.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__10-30.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__15-35.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__20-40.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__25-45.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__30-50.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__35-55.metis 12
gpmetis -objtype=vol srBenchQ3charley5minute__40-60.metis 12


echo ""
echo "******************************"
echo "* running the evaluation 1:1 *"
echo "******************************"

compare_distributions.py --light 12 srBenchQ3charley5minute__00-05.json srBenchQ3charley5minute__00-05.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__05-10.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__10-15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__15-20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__20-25.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__25-30.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__30-35.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__35-40.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__40-45.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__45-50.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__50-55.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__55-60.metis.part.12


echo ""
echo "*********************************************"
echo "* now use previous interval to compare with *"
echo "*********************************************"

compare_distributions.py --light 12 srBenchQ3charley5minute__05-10.json srBenchQ3charley5minute__00-05.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__05-10.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__10-15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__15-20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__20-25.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__25-30.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__30-35.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__35-40.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__40-45.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__45-50.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__50-55.metis.part.12


echo ""
echo "**********************************"
echo "* now use previous two intervals *"
echo "**********************************"

compare_distributions.py --light 12 srBenchQ3charley5minute__10-15.json srBenchQ3charley5minute__00-10.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__05-15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__10-20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__15-25.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__20-30.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__25-35.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__30-40.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__35-45.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__40-50.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__45-55.metis.part.12


echo ""
echo "******************************************"
echo "* now use previous three previous months *"
echo "******************************************"

compare_distributions.py --light 12 srBenchQ3charley5minute__15-20.json srBenchQ3charley5minute__00-15.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__05-20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__10-25.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__15-30.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__20-35.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__25-40.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__30-45.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__35-50.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__40-55.metis.part.12


echo ""
echo "******************************************"
echo "* now use previous four previous months *"
echo "******************************************"

compare_distributions.py --light 12 srBenchQ3charley5minute__20-25.json srBenchQ3charley5minute__00-20.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__25-30.json srBenchQ3charley5minute__05-25.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__30-35.json srBenchQ3charley5minute__10-30.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__35-40.json srBenchQ3charley5minute__15-35.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__40-45.json srBenchQ3charley5minute__20-40.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__45-50.json srBenchQ3charley5minute__25-45.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__50-55.json srBenchQ3charley5minute__30-50.metis.part.12
compare_distributions.py --light 12 srBenchQ3charley5minute__55-60.json srBenchQ3charley5minute__35-55.metis.part.12


cd ..