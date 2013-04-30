package ch.uzh.ddis.katts.monitoring;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.utils.Cluster;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 * This class records the data and stored it to the ZooKeeper.
 * 
 * @author Thomas Hunziker
 * 
 */
public final class Recorder implements TerminationMonitor.TerminationCallback {

	private static Recorder instance;

	/** The path under which the number of triples processed will be stored in zookeeper. */
	public static final String TRIPLES_PROCESSED_PATH = "/triples_processed";

	/** The path under which the number of relevant triples processed will be stored in zookeeper. */
	public static final String RELEVANT_TRIPLES_PROCESSED_PATH = "/relevant_triples_processed";

	/**
	 * For each host there is a subdirecory in this path containing all the sending data for that host.
	 */
	public static final String MESSAGE_RECORDER_PATH = "/message_recorder";
	// public static final String KATTS_STORM_CONFIGURATION_ZK_PATH = "/katts_storm_configuration";

	/**
	 * Whenever all the data for one host has fully been written into zookeeper, we write the name of said host into
	 * this folder. Therefore, if there is an entry for each supervisor host in this path, we can safely assume that all
	 * data has been stored in zookeeper.
	 */
	public static final String MESSAGE_RECORDER_FINISHED_PATH = "/message_recorder_finished";

	private String monitoringPath;

	@SuppressWarnings("rawtypes")
	private Map stormConfiguration;

	/**
	 * We record all messages in this map, as we will access this map from multiple threads in parallel, this the data
	 * structure needs to support this.
	 */
	private AtomicLongMap<SrcDst> messageCounter = AtomicLongMap.create();

	private ZooKeeper zooKeeper;
	private Logger logger = LoggerFactory.getLogger(StarterMonitor.class);

	private Recorder(Map<?, ?> stormConf) {
		this.stormConfiguration = stormConf;

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
		TerminationMonitor.getInstance(stormConfiguration).registerTerminationCallback(this);

	}

	private void createMonitoringRoot() {

		try {
			zooKeeper.create(MESSAGE_RECORDER_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
			zooKeeper.create(MESSAGE_RECORDER_FINISHED_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

	public static synchronized Recorder getInstance(Map<?, ?> stormConf) {

		if (instance == null) {
			instance = new Recorder(stormConf);

			// // Start Virtual Machine Monitoring
			// Thread vmMonitor = new Thread(new VmMonitor(instance, (Long) stormConf.get(VmMonitor.RECORD_INVERVAL)));
			// vmMonitor.start();
		}

		return instance;
	}

	public void recordMessageSending(int sourceTask, int targetTask) {
		// AtomicLongMap is thread safe so we don't need to serialize here
		this.messageCounter.incrementAndGet(new SrcDst(sourceTask, targetTask));
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
	public void workerTerminated() {
		writeCounts();
		logger.info("Message counts are written to ZooKeeper");
	}

	/**
	 * Serialized the provided value and stores the result into zookeeper.
	 * 
	 * @param path
	 *            the path to store the data at.
	 * @param value
	 *            the object to store.
	 */
	public void writeToZookeeper(String path, Serializable value) {
		ZooKeeper zooKeeper;

		try {
			zooKeeper = Cluster.createZooKeeper(this.stormConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("Can't create ZooKeeper instance for monitoring the message sending behaviour.",
					e);
		}

		try {
			int currentIdx = 1;
			// create the path level by level
			while (path.indexOf("/", currentIdx) > 0) {
				currentIdx = path.indexOf("/", currentIdx);
				String p = path.substring(0, currentIdx);

				try {
					zooKeeper.create(p, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} catch (KeeperException e) {
					if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
						logger.warn("Zookeeper node " + p + " already existed.");
					} else {
						throw new RuntimeException("Error while creating Zookeeper node " + p, e);
					}

				}

				currentIdx++;
			}

			zooKeeper.create(path, SerializationUtils.serialize(value), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new RuntimeException("Can't write object to Zookeeper.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted while writing object to Zookeeper.", e);
		}
	}

	private void writeCounts() {
		ZooKeeper zooKeeper;

		try {
			zooKeeper = Cluster.createZooKeeper(stormConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("Can't create ZooKeeper instance for monitoring the message sending behaviour.",
					e);
		}

		for (SrcDst key : this.messageCounter.asMap().keySet()) {

			String sender = Integer.toString(key.srcId);
			String receiver = Integer.toString(key.dstId);
			String count = Long.toString(this.messageCounter.get(key));

			String path = new StringBuilder().append(MESSAGE_RECORDER_PATH).append("/").append(sender).append("___")
					.append(receiver).toString();

			try {
				zooKeeper.create(path, count.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				throw new RuntimeException("Can't write the message count out.", e);
			} catch (InterruptedException e) {
				throw new RuntimeException("Can't write the message count out, because the thread was interrupted.", e);
			}
		}

		String identifier = Cluster.getHostIdentifier();

		try {
			zooKeeper.create(MESSAGE_RECORDER_FINISHED_PATH + "/" + identifier, new byte[0], Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			throw new RuntimeException(String.format("Can't create the finish ZooKeeper entry for host '%1s'.",
					identifier), e);
		} catch (InterruptedException e) {
			throw new RuntimeException(String.format(
					"Can't create the finish ZooKeeper entry for host '%1s, because the thread was interrupted.'.",
					identifier), e);
		}

	}

	/** Helper class used to concurrently record messages that are send from a "source" task to a "destination" task. */
	private class SrcDst {
		// both fields are final, so public access is ok
		public final int srcId;
		public final int dstId;

		public SrcDst(int srcId, int dstId) {
			this.srcId = srcId;
			this.dstId = dstId;
		}

		@Override
		public int hashCode() {
			/*
			 * This is from http://stackoverflow.com/questions/892618/create-a-hashcode-of-two-numbers TODO: read up on
			 * efficient hash functions
			 */
			int hash = 23;
			hash = hash * 31 + srcId;
			hash = hash * 23 + dstId;
			return hash;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true; // check for self-comparison

			if (obj instanceof SrcDst) { // null is no instance, so that's checked also
				SrcDst other = (SrcDst) obj;
				return this.srcId == other.srcId && this.dstId == other.dstId;
			} else {
				return false;
			}
		}
	}

}
