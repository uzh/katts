Revision History
================

3.7.8-SNAPSHOT    - don't print supervisor names during each schedule cycle
3.7.7-SNAPSHOT    - use partition file with number-of-partitions
3.7.6-SNAPSHOT    - only do the scheduling if supervisors could be found.
3.7.5-SNAPSHOT    - do the regular scheduling also
3.7.4-SNAPSHOT    - new metis file scheduler
3.7.2-SNAPSHOT    - use logger in scheduler
3.7.1-SNAPSHOT    - remove zookeeper map (conflict with zookeeper version) --> we use zookeeper 3.3.3 (for storm 0.8.2)
3.7.0-SNAPSHOT    - scheduler test

3.6.4-SNAPSHOT    - make number of workers configurable again

3.6.2-SNAPSHOT    - Support for distributed processing (multi worker capabilities TerminationMonitor capabilities) 

3.5.0             - version used for SSWS/ISWC'13 submission.

3.5.0-SNAPSHOT    - fixed bug in FileGraphPatternReader where it would emit bindings even though not all triple patterns
                    were matched, but only all variables had been bound. 

3.4.2-SNAPSHOT    - json-sendgraph uses real ids and not mapped ones

3.4.1-SNAPSHOT    - graph pattern reader now outputs all matches

3.3.8-SNAPSHOT    - use curator framework to talk to Zookeeper

3.3.6-SNAPSHOT    - fixed bug in recorder that would generate zk-nodes multiple times

3.3.2-SNAPSHOT    - write triple counts to zookeeper

3.2.3-SNAPSHOT    - reverted process method in aggregator

3.2.2-SNAPSHOT    - support for file output in Aggregator

3.1.0-SNAPSHOT    - fromDate and toDate on fileGraphReader

3.0.5-SNAPSHOT    - new program argument "--max-spout-pending"

3.0.4-SNAPSHOT    - increase zookeeper timeout to 30 seconds

3.0.3-SNAPSHOT    - 5k unacked tuples

3.0.2-SNAPSHOT    - copy start and end dates in ExpressionFilterBolt and ExpressionFunctionBolt

3.0.1-SNAPSHOT    - 1 million unacked messages

3.0.0-SNAPSHOT    - Ripped out the "parallelism magic"
                  - bufferTimeout and waitTimeout configurable

2.5.0-SNAPSHOT    - ScheduledThreadPoolExecutor instead of TimerTask
                  - TOPOLOGY_MAX_SPOUT_PENDING set to 100k
                  - 1h ack-timeout
                  - 10 acker executors
                  - 15 minutes wait timeout

2.4.1-SNAPSHOT    - 15 seconds waitTimeout

2.4.0-SNAPSHOT    - new AbstractSynchronizedBold implementation using the SortedTimeoutBuffer

2.3.12-SNAPSHOT   - reformat out-of-order message

2.3.8-SNAPSHOT    - graph reader reads one line at the time until it could emit at least one tuple

2.3.7-SNAPSHOT    - print batch date

2.3.6-SNAPSHOT    - Copy bound variables in bindings

2.3.5-SNAPSHOT    - ExpressionFunction back to normal.

2.3.4-SNAPSHOT    - ExpressionFunction just acks everything...

2.3.3-SNAPSHOT    - remove synchronization from TemporalJoinBolt

2.3.1-SNAPSHOT    - print name of source when messages fail

2.3.1-SNAPSHOT    - moved readToLineNo variable to the source
                  - termination monitoring now supports multiple sources, but still only works correctly if 
                    run on only one machine.

2.2.3-SNAPSHOT    - fileSourcePath as variable in graphreader
                  - new property toLineNo in sourceFile descriptor

2.2.2-SNAPSHOT    - fixed bug: "D" is not a double

2.2.1-SNAPSHOT    - fixed bug in expression filter which would only allow "inherited" variable configuration

2.2.0-SNAPSHOT    - first version of sub graph reader

2.1.5-SNAPSHOT    - don't log acker messages

2.1.4-SNAPSHOT    - using a comparator in ElasticPriorityQueue

2.1.0-SNAPSHOT    - acking facility activated again.
                  - end of run detection depends on acks now

2.0.0-SNAPSHOT    - removed heartbeat

1.3.4-SNAPSHOT    - concurrent messagerecorder

1.3.3-SNAPSHOT    - value converstion does not rely on exceptions anymore.

1.3.2-SNAPSHOT    - new timeformat and loglevel for TerminationBolt
                  - don't blow up if storm info is written to zookeeper multiple times

1.3.1-SNAPSHOT    - parallelism support for TripleFilter

1.3.0             - support for n5 files as source
                  - minAggregator

1.2.0             - store evaluation parameters in google spreadsheet
                  - counts without heartbeat
                  - counts with sources   

1.1.16-SNAPSHOT   - store evaluation parameters in google spreadsheet
                  - counts without heartbeat
                  - counts without sources
            
1.1.15-SNAPSHOT   - counts without heartbeat
                  - counts without sources

1.1.13-SNAPSHOT   - counts without heartbeat 
                  
1.1.12-SNAPSHOT   - counts with heartbeat 
                  - add watcher over and over again                      

1.1.11-SNAPSHOT   - counts with heartbeat 
                  - fast and the furious

1.1.9-SNAPSHOT    - counts with heartbeat 
                  - 5 seconds delay

1.1.8-SNAPSHOT    - counts with heartbeat 
                  - cached file source

1.1.7-SNAPSHOT    - counts with heartbeat 
                  - correct usage of MESSAGE_RECORDER_FINISHED_PATH

1.1.6-SNAPSHOT    counts without heartbeat    

1.1.5-SNAPSHOT    counts with heartbeat

1.1.3-SNAPSHOT    heartbeat
                  new cachedFileSource 

1.1.2-SNAPSHOT    heartbeat
1.1.1-SNAPSHOT    no heartbeat
1.1.0             include evaluation and waiter projects into the main project - no heartbeat
1.0.5             dummy release (lorenz)
1.0.4             dummy release (tom)
1.0.3             include maven repository and scm configuration
1.0.2             use storm 0.8.2
1.0.0             master thesis of TH