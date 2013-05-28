/**
 * This package contains all code for task scheduling (i.e. the process of deciding which task should be assigned
 * to which supervisor). 
 * 
 * <p/>
 * <b>Important:</b> In order for this to work the jar containing the scheduler code needs to be copied to (or linked to)
 * $STORM_HOME/lib. Then the scheduler can be configured by putting the following into the storm.yaml file:
 * 
 * <pre>
 * 	storm.scheduler: "ch.uzh.ddis.katts.scheduler.MetisPartitionFileScheduler"
 * </pre>
 *  
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
package ch.uzh.ddis.katts.scheduler;