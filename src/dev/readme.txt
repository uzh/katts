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

./compare_distributions.py 12 sendgraph288.json sendgraph288.metis.part.12
random traffic: 1083329
partitioned traffic: 593933
improvement: 1.82399193175


For this topology, it would could only speedup things by a factor of 1.5 using 6 machines and 1.3 using 12 machines