package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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

	private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");

	private String jobName;
	private ZooKeeper zooKeeper;
	private TProtocol protocol;
	private Client client;

	private TaskResolver taskResolver;

	private GoogleSpreadsheetHelper googleSpreadsheet;

	private TopologySummary topologySummary = null;
	private StormTopology topology = null;
	private TopologyInfo topologyInfo = null;

	private Map<?, ?> conf;

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
		List<String> children = null;

		try {
			if (zooKeeper.exists(Recorder.MESSAGE_RECORDER_PATH, false) != null) {
				/*
				 * Since the monitoring data only starts to be sent to zookeeper after the query is completed, we need
				 * to wait until this data is written to ZooKeeper.
				 */
				waitForMonitoringData();
				children = zooKeeper.getChildren(Recorder.MESSAGE_RECORDER_PATH, false);
			}
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
		Map<String, Object> data = new HashMap<String, Object>();
		long startTime = Long.valueOf(readValueFromZK(StarterMonitor.KATTS_STARTING_TIME_ZK_PATH));
		long endTime = Long.valueOf(readValueFromZK(TerminationMonitor.KATTS_TERMINATION_ZK_PATH));

		SankeyNetworkDataCollector sankeyHosts = new SankeyNetworkDataCollector();
		SankeyNetworkDataCollector sankeyTasks = new SankeyNetworkDataCollector();
		SankeyNetworkDataCollector sankeyComponents = new SankeyNetworkDataCollector();

		if (children != null) {
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
					messagesSent = new String(zooKeeper.getData(Recorder.MESSAGE_RECORDER_PATH + "/" + child, false,
							null));
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
		}

		// Get total emitted tuples by spouts
		// List<ExecutorSummary> executors = this.topologyInfo.get_executors();
		long numberOfMessagesEmittedBySpouts = 0;
		// for (ExecutorSummary executor : executors) {
		// try {
		// ExecutorSpecificStats specific = executor.get_stats().get_specific();
		// if (specific.get_spout() != null) {
		// Map<String, Long> result = executor.get_stats().get_emitted().get(":all-time");
		// Entry<String, Long> entry = result.entrySet().iterator().next();
		// numberOfMessagesEmittedBySpouts += (long) entry.getValue();
		// }
		//
		// if (specific.get_bolt() != null) {
		// }
		//
		// } catch (RuntimeException e) {
		// // Ignore this exception. This exception can occur because the executor is not a spout
		// log.warn("Could not read spout stats", e);
		// }
		//
		// }

		int numberOfNodes;
		try {
			numberOfNodes = client.getClusterInfo().get_supervisors_size();
		} catch (TException e1) {
			throw new RuntimeException("Could not connect to the nimbus host. Does the cluster running?", e1);
		}
		long numberOfProcessors = (Long) conf.get(TopologyBuilder.NUMBERS_OF_PROCESSORS);
		long numberOfProcessorsPerNode = 0;
		try {
			if (numberOfNodes == 0) {
				numberOfNodes = 1;
			}
			numberOfProcessorsPerNode = numberOfProcessors / numberOfNodes;
		} catch (Exception e) {
			log.error(e.getMessage());
		}

		data.put("job-name", jobName);
		data.put("start-date", this.df.format(new Date(startTime)));
		data.put("end-date", this.df.format(new Date(endTime)));
		data.put("duration", (endTime - startTime) / 1000);
		data.put("total-messages-processed", totalMessages);
		data.put("total-remote-messages", totalRemoteMessages);
		data.put("total-local-messages", totalLocalMessages);
		data.put("number-of-nodes", numberOfNodes);
		data.put("number-of-processors", numberOfProcessors);
		data.put("number-of-processors-per-node", numberOfProcessorsPerNode);
		data.put("expected-number-of-tasks", (Long) conf.get(TopologyBuilder.KATTS_EXPECTED_NUMBER_OF_EXECUTORS));
		data.put("factor-of-threads-per-processor", (Double) conf.get(TopologyBuilder.KATTS_FACTOR_OF_THREADS_CONFIG));
		data.put("number-of-triples-processed", numberOfMessagesEmittedBySpouts); // that's wrong! no?
		data.put("sankey-tasks-json", sankeyTasks.toJson());
		data.put("sankey-components-json", sankeyComponents.toJson());
		data.put("sankey-hosts-json", sankeyHosts.toJson());

		// data.put("number-of-tuples-outputed", jobData.getNumberOfTuplesOutputed());
		// job.setNumberOfTuplesOutputed(Long.valueOf(readValueFromZK(TerminationMonitor.KATTS_TUPLES_OUTPUTTED_ZK_PATH)));

		try {
			googleSpreadsheet.addRow(data);
		} catch (Exception e) {
			log.error("Error while trying to write to Google spreadsheet", e);
		}
	}

	/**
	 * This method blocks until all monitoring data has been written into Zookeeper before returning.
	 * 
	 * @throws InterruptedException
	 *             if the thread that entered this method is interrupted by some external event (for example when the
	 *             current java vm gets shutdown early).
	 */
	private void waitForMonitoringData() throws InterruptedException {
		int supervisorSize = 0;
		try {
			supervisorSize = this.client.getClusterInfo().get_supervisors_size();
		} catch (TException e1) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e1);
		}

		MonitoringFinishedWaiter waiter = new MonitoringFinishedWaiter(supervisorSize);

		waiter.waitUntilFinished();
	}

	private class MonitoringFinishedWaiter implements Watcher {

		/**
		 * We use a CountDownLatch with a count of 1, which we will decrease by one as soon as we have the data of all
		 * hosts in zookeeper. All threads entering {@link #waitUntilFinished()} will be blocked until this count is
		 * decreased to one.
		 */
		private CountDownLatch latch = new CountDownLatch(1);

		/**
		 * We need to wait until all supervisors have written their log data into zookeeper. This number specifies for
		 * how many entries we need to wait.
		 */
		private int supervisorSize;

		public MonitoringFinishedWaiter(int supervisorSize) {
			this.supervisorSize = supervisorSize;

			try { // attach watcher
				zooKeeper.getChildren(Recorder.MESSAGE_RECORDER_FINISHED_PATH, this);
			} catch (KeeperException e) {
				throw new RuntimeException("Can't access the monitoring finish znode.", e);
			} catch (InterruptedException e) {
				throw new RuntimeException("Can't access the monitoring finish znode, the thread was interrupted.", e);
			}
			checkIfFinished(); // check if we should already release the lock

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
			List<String> children = null;
			try {
				// TODO this is probably adding the same watcher over and over again...
				children = zooKeeper.getChildren(Recorder.MESSAGE_RECORDER_FINISHED_PATH, this);
			} catch (KeeperException e) {
				throw new RuntimeException("Can't access the monitoring finish znode.", e);
			} catch (InterruptedException e) {
				throw new RuntimeException("Can't access the monitoring finish znode, the thread was interrupted.", e);
			}
			System.out.println(String.format("waiting for results: %d of %d arrived, already.", children.size(),
					this.supervisorSize));
			if (children.size() >= this.supervisorSize) {
				latch.countDown();
			}
		}

		/**
		 * This method blocks until all monitoring data has been written into Zookeeper before returning.
		 * 
		 * @throws InterruptedException
		 *             if the thread that entered this method is interrupted by some external event (for example when
		 *             the current java vm gets shutdown early).
		 */
		public void waitUntilFinished() throws InterruptedException {
			this.latch.await();
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
			conf = (Map<?, ?>) JSONValue.parse(client.getTopologyConf(this.topologyInfo.get_id()));
		} catch (NotAliveException e1) {
			throw new RuntimeException(
					"Could not read the topology configuration, because the topology seems not to be active.", e1);
		} catch (TException e1) {
			throw new RuntimeException("The connection to the cluster seems to be broken. Does the nimbus run?", e1);
		}

	}

	public GoogleSpreadsheetHelper getGoogleSpreadsheet() {
		return googleSpreadsheet;
	}

	public void setGoogleSpreadsheet(GoogleSpreadsheetHelper googleSpreadsheet) {
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
