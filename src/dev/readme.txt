This folder contains scripts and files for testing the effectiveness of partitioning the tasks using the Metis graph
partitioner.



./compare_distributions.py 6 sendgraph.json sendgraph.metis.part.6random
traffic: 35.720.014
partitioned traffic: 23.785.672
improvement: 1.5017450001

./compare_distributions.py 12 sendgraph.json sendgraph.metis.part.12random
random traffic: 39408855
partitioned traffic: 30904227
improvement: 1.27519303427



./compare_distributions.py 12 sendgraph144hb.json sendgraph144hb.metis.part.12
random traffic: 50413586
partitioned traffic: 46788707
improvement: 1.07747337408

./compare_distributions.py 12 sendgraph144nohb.json sendgraph144nohb.metis.part.12
random traffic: 41237115
partitioned traffic: 30128635
improvement: 1.36870173508



./compare_distributions.py 12 sendgraph48hb.json sendgraph48hb.metis.part.12 
random traffic: 39857638
partitioned traffic: 37969747
improvement: 1.0497209265

./compare_distributions.py 12 sendgraph48nohb.json sendgraph48nohb.metis.part.12 
random traffic: 41113946
partitioned traffic: 33714700
improvement: 1.21946646418



./compare_distributions.py 12 sendgraph12nhns.json sendgraph12nhns.metis.part.12 
random traffic: 13862474
partitioned traffic: 9940791
improvement: 1.39450411944

./compare_distributions.py 12 sendgraph48nhns.json sendgraph48nhns.metis.part.12 
random traffic: 13398916
partitioned traffic: 9567403
improvement: 1.40047576129

./compare_distributions.py 12 sendgraph144nhns.json sendgraph144nhns.metis.part.12 
random traffic: 13505145
partitioned traffic: 10078019
improvement: 1.34005948987



For this topology, it would could only speedup things by a factor of 1.5 using 6 machines and 1.3 using 12 machines