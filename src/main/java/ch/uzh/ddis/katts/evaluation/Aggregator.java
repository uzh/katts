package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.Nimbus.Client.Factory;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.monitoring.Recorder;
import ch.uzh.ddis.katts.monitoring.StarterMonitor;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;
import ch.uzh.ddis.katts.utils.Cluster;

/**
 * This class tasks the information from storm and zookeeper to build the aggreates that are pushed to the spreadsheet.
 * 
 * @author Thomas Hunziker
 * 
 */
class Aggregator {

	private final Logger log = LoggerFactory.getLogger(StarterMonitor.class);

	private String jobName;
	private ZooKeeper zooKeeper;
	private TProtocol protocol;
	private Client client;

	private TaskResolver taskResolver;

	private GoogleSpreadsheet googleSpreadsheet;

	private TopologySummary topologySummary = null;
	private StormTopology topology = null;
	private TopologyInfo topologyInfo = null;

	@SuppressWarnings("rawtypes")
	private Map conf;

	public static final String KATTS_EVALUATION_COMPLETED_ZK_PATH = "/katts_evaluation_completed";

	public Aggregator(TProtocol protocol, String jobName) {
		this.jobName = jobName;
		this.protocol = protocol;
		Factory factory = new Nimbus.Client.Factory();
		client = factory.getClient(this.protocol);

		loadTopologyObjects();
		getStormConfig();
		getZooKeeper();

		taskResolver = new TaskResolver(topologyInfo);
	}

	public void aggregateMessagePerHost() {

		// Since the monitoring data is written, when the query is completed, we need to wait until this data is written
		// to ZooKeeper.
		waitForMonitoringData();

		List<String> children = null;
		try {
			children = zooKeeper.getChildren(Recorder.KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH, false);
		} catch (KeeperException e) {
			throw new RuntimeException("Could not load the monitoring message records from ZooKeeper.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Could not load the monitoring message records from ZooKeeper, because the thread was interrupted.",
					e);
		}
		long totalRemoteMessages = 0;
		long totalLocalMessages = 0;
		long totalMessages = 0;

		SankeyNetworkDataCollector sankeyHosts = new SankeyNetworkDataCollector();
		SankeyNetworkDataCollector sankeyTasks = new SankeyNetworkDataCollector();
		SankeyNetworkDataCollector sankeyComponents = new SankeyNetworkDataCollector();

		for (String child : children) {

			String[] splits = child.split("___");
			String senderTaskId = splits[0];
			String receiverTaskId = splits[1];
			String messagesSent = null;

			String sourceHost = taskResolver.getHostnameByTask(senderTaskId);
			String destinationHost = taskResolver.getHostnameByTask(receiverTaskId);

			String sourceComponent = taskResolver.getComponentIdByTask(senderTaskId);
			String destinationComponent = taskResolver.getComponentIdByTask(receiverTaskId);

			if (sourceHost == null) {
				sourceHost = "undefined-host-name";
			}

			if (destinationHost == null) {
				destinationHost = "undefined-host-name";
			}

			if (sourceComponent == null) {
				sourceComponent = "undefined-component";
			}

			if (destinationComponent == null) {
				destinationComponent = "undefined-component";
			}

			try {
				messagesSent = new String(zooKeeper.getData(Recorder.KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH + "/"
						+ child, false, null));
			} catch (KeeperException e) {
				throw new RuntimeException("Could not read the message cound from the znode.", e);
			} catch (InterruptedException e) {
				throw new RuntimeException(
						"Could not read the message cound from the znode, because the thread was interrupted.", e);
			}

			long messageCount = Long.valueOf(messagesSent);

			// Build sankey maps
			sankeyTasks.updateEdgeWeights(senderTaskId, receiverTaskId, messageCount);
			sankeyHosts.updateEdgeWeights(sourceHost, destinationHost, messageCount);
			sankeyComponents.updateEdgeWeights(sourceComponent, destinationComponent, messageCount);

			if (sourceHost.equals(destinationHost)) {
				totalLocalMessages = totalLocalMessages + messageCount;
			} else {
				totalRemoteMessages = totalRemoteMessages + messageCount;
			}
			totalMessages = totalMessages + messageCount;
		}

		// Get total emitted tuples by spouts
		List<ExecutorSummary> executors = this.topologyInfo.get_executors();
		long numberOfMessagesEmittedBySpouts = 0;
		for (ExecutorSummary executor : executors) {
			try {
			ExecutorSpecificStats specific = executor.get_stats().get_specific();
				if (specific.get_spout() != null) {
					Map<String, Long> result = executor.get_stats().get_emitted().get(":all-time");
					Entry<String, Long> entry = result.entrySet().iterator().next();
					numberOfMessagesEmittedBySpouts += (long) entry.getValue();
				}

				if (specific.get_bolt() != null) {
				}

			} catch (RuntimeException e) {
				// Ignore this exception. This exception can occur because the executor is not a spout
				log.warn("Could not read spout stats", e);
			}

		}

		int numberOfNodes;
		try {
			numberOfNodes = client.getClusterInfo().get_supervisors_size();
		} catch (TException e1) {
			throw new RuntimeException("Could not connect to the nimbus host. Does the cluster running?", e1);
		}
		long numberOfProcessors = (Long) conf.get(TopologyBuilder.NUMBERS_OF_PROCESSORS);
		long numberOfProcessorsPerNode = 0;
		try {
			numberOfProcessorsPerNode = numberOfProcessors / numberOfNodes;
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		JobData job = new JobData();

		job.setJobName(this.jobName);
		job.setExpectedNumberOfTasks((Long) conf.get(TopologyBuilder.KATTS_EXPECTED_NUMBER_OF_EXECUTORS));
		job.setJobEnd(Long.valueOf(readValueFromZK(TerminationMonitor.KATTS_TERMINATION_ZK_PATH)));
		job.setJobStart(Long.valueOf(readValueFromZK(StarterMonitor.KATTS_STARTING_TIME_ZK_PATH)));
		job.setNumberOfNodes(numberOfNodes);
		job.setNumberOfProcessors(numberOfProcessors);
		job.setNumberOfProcessorsPerNode(numberOfProcessorsPerNode);
		// job.setNumberOfTuplesOutputed(Long.valueOf(readValueFromZK(TerminationMonitor.KATTS_TUPLES_OUTPUTTED_ZK_PATH)));
		job.setTotalLocalMessages(totalLocalMessages);
		job.setTotalRemoteMessages(totalRemoteMessages);
		job.setTotalMessages(totalMessages);
		job.setFactorOfThreadsPerProcessor((Double) conf.get(TopologyBuilder.KATTS_FACTOR_OF_THREADS_CONFIG));
		job.setNumberOfTriplesProcessed(numberOfMessagesEmittedBySpouts);
		job.setSankeyDataComponents(sankeyComponents.toJson());
		job.setSankeyDataHosts(sankeyHosts.toJson());
		job.setSankeyDataTasks(sankeyTasks.toJson());

		try {
			googleSpreadsheet.addRow(job);
		} catch (Exception e) {
			log.error("Error while trying to write to Google spreadsheet", e);
		}
	}

	private void waitForMonitoringData() {
		int supervisorSize = 0;
		try {
			supervisorSize = this.client.getClusterInfo().get_supervisors_size();
		} catch (TException e1) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e1);
		}

		MonitoringFinishedWaiter waiter = new MonitoringFinishedWaiter(supervisorSize);

		List<String> children = null;
		try {
			children = zooKeeper.getChildren(Recorder.KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH, waiter);
		} catch (KeeperException e) {
			throw new RuntimeException("Cant access the znode for monitoring finished barrier.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Cant access the znode for monitoring finished barrier, because the thread was interrupted.", e);
		}

		if (children.size() >= supervisorSize) {
			return;
		} else {
			waiter.run();
		}

	}

	private class MonitoringFinishedWaiter implements Watcher, Runnable {

		private Boolean isFinished = new Boolean(false);
		private int supervisorSize = 0;

		public MonitoringFinishedWaiter(int supervisorSize) {
			this.supervisorSize = supervisorSize;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == Event.EventType.None) {
				switch (event.getState()) {
				case Expired:
					checkIfFinished();
					break;
				}
			} else {
				checkIfFinished();
			}
		}

		private void checkIfFinished() {
			synchronized (isFinished) {
				List<String> children = null;
				try {
					children = zooKeeper.getChildren(Recorder.KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH, this);
				} catch (KeeperException e) {
					throw new RuntimeException("Can't access the monitoring finish znode.", e);
				} catch (InterruptedException e) {
					throw new RuntimeException("Can't access the monitoring finish znode, the thread was interrupted.",
							e);
				}

				if (children.size() >= this.supervisorSize) {
					isFinished = true;
					isFinished.notify();
				}
			}
		}

		@Override
		public void run() {
			synchronized (isFinished) {
				try {
					isFinished.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException("Can't wait for finishing the monitoring, the thread was interrupted.",
							e);
				}
			}
		}

	}

	private String readValueFromZK(String path) {

		try {
			return new String(zooKeeper.getData(path, false, null));
		} catch (KeeperException e) {
			throw new RuntimeException(String.format("Could not read from the znode %1s.", path), e);
		} catch (InterruptedException e) {
			throw new RuntimeException(String.format(
					"Could not read from the znode %1s, because the thread was interrupted.", path), e);
		}

	}

	private void loadTopologyObjects() {
		List<TopologySummary> topologies;
		try {
			topologies = client.getClusterInfo().get_topologies();
		} catch (TException e1) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e1);
		}

		for (TopologySummary topology : topologies) {
			if (topology.get_name().equalsIgnoreCase(jobName)) {
				this.topologySummary = topology;
				break;
			}
		}

		if (topologySummary == null) {
			throw new RuntimeException(String.format("No topology summary found in the cluster with the name '%1s'.",
					jobName));
		}

		try {
			topology = client.getTopology(topologySummary.get_id());
		} catch (NotAliveException e) {
			throw new RuntimeException(
					"The given topology (by the job name) is not alive in the cluster, and there for not be evaluatable.",
					e);
		} catch (TException e) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e);
		}

		if (topology == null) {
			throw new RuntimeException(String.format("No topology found in the cluster with the name '%1s'.", jobName));
		}

		try {
			topologyInfo = client.getTopologyInfo(topologySummary.get_id());
		} catch (NotAliveException e) {
			throw new RuntimeException(
					"The given topology (by the job name) is not alive in the cluster, and there for not be evaluatable.",
					e);
		} catch (TException e) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e);
		}

		if (topologyInfo == null) {
			throw new RuntimeException(String.format("No topology found in the cluster with the name '%1s'.", jobName));
		}
	}

	private void getZooKeeper() {
		try {
			zooKeeper = Cluster.createZooKeeper(conf);
		} catch (IOException e) {
			throw new RuntimeException("Failed to initialize the ZooKeeper instance.", e);
		}
	}

	private void getStormConfig() {

		try {
			conf = (Map) JSONValue.parse(client.getTopologyConf(this.topologyInfo.get_id()));
		} catch (NotAliveException e1) {
			throw new RuntimeException(
					"Could not read the topology configuration, because the topology seems not to be active.", e1);
		} catch (TException e1) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e1);
		}

	}

	public GoogleSpreadsheet getGoogleSpreadsheet() {
		return googleSpreadsheet;
	}

	public void setGoogleSpreadsheet(GoogleSpreadsheet googleSpreadsheet) {
		this.googleSpreadsheet = googleSpreadsheet;
	}

	public void sendFinishSignal() {

		try {
			zooKeeper.create(KATTS_EVALUATION_COMPLETED_ZK_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new RuntimeException("Can't write the evaluation completed znode.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Can't write the evaluation completed znode., because the thread was interrupted.", e);
		}
	}

}
