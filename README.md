katts
=====

KATTS

Revision History
================
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


      





     
    


       



                  



                  
                  


                  



                  

         
                  