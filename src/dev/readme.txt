This folder contains scripts and files for testing the effectiveness of partitioning the tasks using the Metis graph
partitioner.

# no volume did better this time??
gpmetis srBenchQ3charley12-48Task.metis 12 ; ./compare_distributions.py 12 srBenchQ3charley12-48Task.json srBenchQ3charley12-48Task.metis.part.12

total messages:      476330064
uniform traffic:     438325165
partitioned traffic: 417521650
improvement:         4.75%
uniform load:        [43182323, 41881784, 39226113, 38567323, 48364254, 38514892, 40053446, 38387625, 34322689, 37448749, 37554156, 38826710]
mean, stdv, relstdv: 39694172.00,3517046.68,8.86%
partitioned load:    [40874227, 40372892, 40821344, 39317075, 40748258, 39548179, 38735400, 36684205, 40595277, 38621752, 40709465, 39301990]
mean, stdv, relstdv: 39694172.00,1262183.08,3.18%






./compare_distributions.py 12 srBenchQ3smallTask.json srBenchQ3smallTask.metis.part.12 
total messages:      40351960
uniform traffic:     37907329
partitioned traffic: 34479976
improvement:         9.04%
uniform load:        [3396857, 3791020, 3349880, 3436287, 2701148, 3572667, 3438278, 2637416, 3252654, 3813530, 3168030, 3794193]
mean, stdv, relstdv: 3362663.33,386491.28,11.49%
partitioned load:    [3350140, 3321460, 3371095, 3400740, 3430886, 3356483, 3366734, 3367802, 3408842, 3355940, 3292714, 3329124]


#################### reverse sorting below ######################

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