/**
 * 
 */
package ch.uzh.ddis.katts.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

/**
 * This scheduler partitions the topologies according to the definition that is provided in the form of a METIS
 * partition file that follows the name pattern:
 * 
 * <pre>
 * [topology-name].metis.part.[number-of-supervisors]
 * 
 * Example: opengov2001.metis.part.6
 * </pre>
 * 
 * The partition file has to be in the current working directory and have the format that is specified by the METIS,
 * which is defined as:
 * 
 * <pre>
 * The partition file of a graph with n vertices consists of n lines with a single number per line. The ith line of the 
 * file contains the partition number that the ith vertex belongs to. Partition numbers start from 0 up to the number of
 * partitions minus one.
 * </pre>
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class MetisPartitionFileScheduler implements IScheduler {

	/** This default scheduler will be used if no partition file for a topology could be found. */
	private EvenScheduler evenScheduler;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map conf) {
		this.evenScheduler = new EvenScheduler();
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		ArrayList<SupervisorDetails> supervisors;

		supervisors = new ArrayList<SupervisorDetails>();
		supervisors.addAll(cluster.getSupervisors().values());
		
		if (supervisors.isEmpty()) {
			System.out.println("No supervisors found! Aborting scheduling process");
		} else {

			for (TopologyDetails topology : topologies.getTopologies()) {
				File partitionFile = new File(String.format("%1s.metis.part.%1d", topology.getName(),
						supervisors.size()));

				if (partitionFile.exists()) {
					if (cluster.needsScheduling(topology)) {
						ArrayList<Integer> taskToSupervisorAssignment; // index = task_id, value = partition
						SetMultimap<SupervisorDetails, ExecutorDetails> supervisorToExecutor;
						supervisorToExecutor = HashMultimap.create();

						System.out.println("\n\nSCHEDULING: " + topology.getName());

						// the number of task should always equal the number of executors in katts
						taskToSupervisorAssignment = new ArrayList<Integer>();

						// read partition file
						try {
							String line; // the current line being read
							BufferedReader partitionFileReader;

							partitionFileReader = new BufferedReader(new FileReader(partitionFile));

							line = partitionFileReader.readLine();
							while (line != null) {
								taskToSupervisorAssignment.add(Integer.valueOf(line));
								line = partitionFileReader.readLine();
							}
							partitionFileReader.close();
							System.out.println("Scheduler read " + taskToSupervisorAssignment.size() + " lines");
						} catch (Exception e) {
							System.out.println("Error reading partition file.");
							e.printStackTrace();
						}

						for (ExecutorDetails executor : topology.getExecutors()) {
							if (executor.getStartTask() != executor.getEndTask()) {
								throw new IllegalStateException("Found one executor that is executing multiple tasks.");
							}
							// here the assignment of task to supervisor happens
							int taskId; // the task we're currently finding an assingment for
							int assignedSupervisorIndex; // the index of the assigned supervisor within the supervisors
															// ArrayList
							SupervisorDetails supervisor; // the assigned Supervisor

							taskId = executor.getStartTask();
							// assignedSupervisorIndex = taskId % supervisors.size(); // here the assignment is made
							assignedSupervisorIndex = taskToSupervisorAssignment.get(taskId);
							supervisor = supervisors.get(assignedSupervisorIndex);

							System.out.println("task " + taskId + " goes to host " + supervisor.getHost() + " ("
									+ assignedSupervisorIndex + ")");
							supervisorToExecutor.get(supervisor).add(executor);
						}

						for (SupervisorDetails supervisor : supervisors) {
							List<WorkerSlot> workerSlots;

							workerSlots = cluster.getAvailableSlots(supervisor);
							if (workerSlots.isEmpty()) {
								for (Integer port : cluster.getUsedPorts(supervisor)) {
									cluster.freeSlot(new WorkerSlot(supervisor.getId(), port));
								}
							}

							// re-get available worker slots
							workerSlots = cluster.getAvailableSlots(supervisor);
							// we have always only one worker slot per supervisor in katts
							cluster.assign(workerSlots.get(0), topology.getId(), supervisorToExecutor.get(supervisor));
						}
					}
				} else {
					throw new RuntimeException("Could not find a partition file at " + partitionFile.getAbsolutePath());
				}

			}
		}

		// do regular scheduling for all topologies
		evenScheduler.schedule(topologies, cluster);
	}

}
