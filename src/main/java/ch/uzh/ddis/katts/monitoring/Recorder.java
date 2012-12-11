package ch.uzh.ddis.katts.monitoring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Map;

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.utils.Cluster;

/**
 * This class records the data and stored it to the ZooKeeper.
 * 
 * @author Thomas Hunziker
 *
 */
public final class Recorder implements TerminationWatcher {

	private static Recorder instance;

	public static final String KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH = "/katts_message_monitoring";
	public static final String KATTS_STORM_CONFIGURATION_ZK_PATH = "/katts_storm_configuration";
	public static final String KATTS_MONITORING_FINISHED_ZK_ROOT_PATH = "/katts_monitoring_finished";

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
		createFinishMonitorRoot();

		// Currently we write out the results, when we get the signal that the query was completed. This may be changed
		// in future.
		TerminationMonitor.getInstance(stormConfiguration).addTerminationWatcher(this);

	}

	private void createMonitoringRoot() {

		try {
			zooKeeper.create(KATTS_MESSAGE_MONITORING_ZK_ROOT_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
				logger.info("The root monitoring entry was already created.");
			} else {
				throw new RuntimeException("Can't create the root monitoring ZooKeeper entry.", e);
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Can't create the root monitoring ZooKeeper entry, because the thread was interrupted.", e);
		}

	}
	
	private void createFinishMonitorRoot() {

		// Write the root path for the monitoring termination barrier
		try {
			zooKeeper.create(KATTS_MONITORING_FINISHED_ZK_ROOT_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
				logger.info("The root monitoring finish entry was already created.");
			} else {
				throw new RuntimeException("Can't create the root monitoring finish ZooKeeper entry.", e);
			}

		} catch (InterruptedException e) {
			throw new RuntimeException(
					"Can't create the root monitoring finish ZooKeeper entry, because the thread was interrupted.", e);
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

	}

	public String getMonitoringPath() {
		return monitoringPath;
	}

	public void setMonitoringPath(String monitoringPath) {
		this.monitoringPath = monitoringPath;
	}

	@Override
	public void terminated() {
		writeCounts();
		logger.info("Message counts are written to ZooKeeper");	
	}

	private void writeCounts() {
		MapIterator it = messageCounter.mapIterator();

		ZooKeeper zooKeeper;
		
		try {
			zooKeeper = Cluster.createZooKeeper(stormConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("Can't create ZooKeeper instance for monitoring the message sending behaviour.",
					e);
		}

		
		
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
		
		String identifier = getHostIdentifier();
		
		try {
			zooKeeper.create(KATTS_MONITORING_FINISHED_ZK_ROOT_PATH + "/" + identifier, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new RuntimeException(String.format("Can't create the finish ZooKeeper entry for host '%1s'.", identifier), e);
		} catch (InterruptedException e) {
			throw new RuntimeException(
					String.format("Can't create the finish ZooKeeper entry for host '%1s, because the thread was interrupted.'.", identifier), e);
		}
		
	}
	
	/**
	 * This method returns the nodes host on which this code is executed on.
	 * 
	 * @return
	 */
	private String getHostIdentifier() {
		
		try {
			return  InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			logger.info("Can't get hostname. Instead the IP address is used.");
		}
		
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.info("Can't get ip address. Instead a random number is used.");
		}
		
		SecureRandom random = new SecureRandom();
		return Long.toString(random.nextLong());
	}

}
