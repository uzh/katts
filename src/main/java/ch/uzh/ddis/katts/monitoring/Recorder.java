package ch.uzh.ddis.katts.monitoring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.utils.Cluster;

public final class Recorder implements Watcher {

	private static Recorder instance;

	public static final String KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH = "katts_message_monitoring";
	public static final String KATTS_STORM_CONFIGURATION_ZK_PATH = "katts_storm_configuration";

	private String monitoringPath;
	private String topologyName;

	@SuppressWarnings("rawtypes")
	private Map stormConfiguration;


	private MultiKeyMap messageCounter = new MultiKeyMap();

	private ZooKeeper zooKeeper;
	private Logger logger = LoggerFactory.getLogger(StarterMonitor.class);

	private Recorder(Map stormConf, String topologyName) {
		this.stormConfiguration = stormConf;
		this.topologyName = topologyName;

		try {
			zooKeeper = Cluster.createZooKeeper(stormConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("Can't create ZooKeeper instance for monitoring the message sending behaviour.",
					e);
		}
		
		createMonitoringRoot();
		writeStormConfigurationToZooKeeper();

		// Currently we write out the results, when we get the signal that the query was completed. This may be changed
		// in future.
		TerminationMonitor.getInstance(stormConfiguration).addTerminationWatcher(this);

	}
	
	private void writeStormConfigurationToZooKeeper() {

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(stormConfiguration);
		} catch (IOException e1) {
			throw new RuntimeException("Can't serialize the storm configuration for later usage.");
		}

		try {
			zooKeeper.create(KATTS_STORM_CONFIGURATION_ZK_PATH, bos.toByteArray(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code().equals("KeeperException.NodeExists")) {
				logger.info("The storm configuration znode was already created.");
			} else {
				throw new RuntimeException("Can't create the storm configuration ZooKeeper znode.", e);
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Can't create the storm configuration znode, because the thread was interrupted.", e);
		}
		
	}
	
	private void createMonitoringRoot() {

		try {
			zooKeeper.create(KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code().equals("KeeperException.NodeExists")) {
				logger.info("The root monitoring entry was already created.");
			} else {
				throw new RuntimeException("Can't create the root monitoring ZooKeeper entry.", e);
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Can't create the root monitoring ZooKeeper entry, because the thread was interrupted.", e);
		}
		
	}

	public static synchronized Recorder getInstance(Map stormConf, String topologyName) {

		if (instance == null) {
			instance = new Recorder(stormConf, topologyName);

			// Start Virtual Machine Monitoring
			Thread vmMonitor = new Thread(new VmMonitor(instance, (Long) stormConf.get(VmMonitor.RECORD_INVERVAL)));
			vmMonitor.start();
		}

		return instance;
	}

	public synchronized void recordMessageSending(int sourceTask, int targetTask) {

		Long counter = (Long) messageCounter.get(sourceTask, targetTask);
		if (counter == null) {
			counter = 1l;
		} else {
			counter++;
		}

		messageCounter.put(sourceTask, targetTask, counter);

	}

	public synchronized void recordMemoryStats(long maxMemory, long allocatedMemory, long freeMemory) {
		// TODO: Find a way to log this data in some other way.

		// String[] line = new String[5];
		// line[0] = Long.toString(System.currentTimeMillis());
		// line[1] = this.getHostName();
		// line[2] = Long.toString(maxMemory);
		// line[3] = Long.toString(allocatedMemory);
		// line[4] = Long.toString(freeMemory);
		//
		// vmStatsWriter.writeNext(line);
		// try {
		// vmStatsWriter.flush();
		// } catch (IOException e) {
		// throw new RuntimeException(e);
		// }

	}

	public String getMonitoringPath() {
		return monitoringPath;
	}

	public void setMonitoringPath(String monitoringPath) {
		this.monitoringPath = monitoringPath;
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
			// The state of the connection has changed
			switch (event.getState()) {
			case SyncConnected:
				// Nothing to do, when the file is written, then we are finished.
				break;
			case Expired:
				writeCounts();
				break;
			}
		} else {
			if (path != null && path.equals(TerminationMonitor.KATTS_TERMINATION_ZK_PATH)) {
				writeCounts();
			}
		}
	}

	private void writeCounts() {
		MapIterator it = messageCounter.mapIterator();

		// The final message count file structure is: sourceTaskId, destinationTaskId, messageCount

		while (it.hasNext()) {
			MultiKey next = (MultiKey) it.next();
			Object[] keys = next.getKeys();

			String sender = ((Integer) keys[0]).toString();
			String receiver = ((Integer) keys[1]).toString();
			String count = Long.toString((Long) messageCounter.get(next));

			String path = new StringBuilder().append(KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH).append("/").append(sender)
					.append("___").append(receiver).toString();

			try {
				zooKeeper.create(path, count.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				throw new RuntimeException("Can't write the message count out.", e);
			} catch (InterruptedException e) {
				throw new RuntimeException("Can't write the message count out, because the thread was interrupted.", e);
			}

		}
	}

}
