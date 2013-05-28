#!/bin/bash
#
# This script does it all:
#  convert json to metis
#  partition metis file into 12 partitions
#  simulate and compare performance
#

python json2metis.py $1.json > $1.metis
gpmetis -objtype=vol $1.metis 6
#gpmetis -objtype=cut $1.metis 12
python compare_distributions.py 6 $1.json $1.metis.part.6
