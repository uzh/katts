package ch.uzh.ddis.katts.monitoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.utils.Cluster;

/**
 * This monitor checks when the query is terminated.
 * 
 * @see Termination
 * 
 * @author Thomas Hunziker
 * 
 */
public class TerminationMonitor implements Watcher {

	private static TerminationMonitor instance;

	private ZooKeeper zooKeeper;

	private Map stormConf;

	/** A flag to remember if this monitor has "told" the Zookeeper that all its sources have finished processing. */
	private boolean isTerminated = false;

	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";
	public static final String KATTS_TERMINATION_ZK_PATH = "/katts_terminated";
	public static final String KATTS_TUPLES_OUTPUTTED_ZK_PATH = "/katts_number_of_tuples_outputed";

	private List<TerminationWatcher> watchers = new ArrayList<TerminationWatcher>();

	private Logger logger = LoggerFactory.getLogger(TerminationMonitor.class);

	/**
	 * We keep track of all sources that have not yet procesed all of their content using this set. Whenever a new
	 * source registers itself usding the {@link #registerSource(String)} method, the id of the source gets added to
	 * this set. Whenever a source tells the monitor that it is done processing using the {@link #terminate(String)} we
	 * remove this source from this set.
	 * <p/>
	 * The worker instance this monitor runs on is thought to have fully finished processing when this set is empty
	 * after its {@link #terminate(String)} has been called.
	 */
	private Set<String> unfinishedSources = Collections.synchronizedSet(new HashSet<String>());

	private TerminationMonitor(Map stormConf) {
		this.stormConf = stormConf;
		this.connect();

	}

	public static synchronized TerminationMonitor getInstance(Map stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	private synchronized void connect() {
		try {
			zooKeeper = Cluster.createZooKeeper(stormConf);
		} catch (IOException e1) {
			throw new RuntimeException("Could not connect to ZooKeeper.", e1);
		}
		try {
			// tell zookeeper, that we want to be informed about changes to this value
			zooKeeper.exists(KATTS_TERMINATION_ZK_PATH, this);
		} catch (KeeperException e) {
			throw new RuntimeException("Could not add watcher on the termination znode on ZooKeeper.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not add watcher on the termination znode on ZooKeeper.", e);
		}
	}

	public synchronized void addTerminationWatcher(TerminationWatcher watcher) {
		watchers.add(watcher);
	}

	/**
	 * Tells the termination monitor that the source with the given id has processed all its input.
	 * 
	 * @param sourceId
	 *            the source which has processed all its input.
	 */
	public synchronized void terminate(String sourceId) {
		// as the unfinishedSources set is synchronized, this method does not need to be synchronized.
		this.unfinishedSources.remove(sourceId);

		logger.info("Source with id " + sourceId + " is done processing. Still waiting for others: "
				+ this.unfinishedSources);

		/*
		 * If this method is invoked multiple times, we need to make sure, that the barrier is only written the first
		 * time.
		 * 
		 * TODO lorenz: This only works for as long as we're only running on one machine! Fix it!
		 */
		if (!this.isTerminated && this.unfinishedSources.isEmpty()) {
			logger.info("Run terminated.");

			ZooKeeper zooKeeper;
			try {
				zooKeeper = Cluster.createZooKeeper(stormConf);
			} catch (IOException e1) {
				throw new RuntimeException("Could not create ZooKeeper instance.", e1);
			}

			try {
				zooKeeper.create(KATTS_TERMINATION_ZK_PATH, Long.toString(System.currentTimeMillis()).getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
					throw new RuntimeException(
							"Can't create the termination barrier in ZooKeeper, because there was already one.", e);
				} else {
					throw new RuntimeException("Can't create the termination ZooKeeper entry.", e);
				}
			} catch (Exception e) {
				throw new RuntimeException("The termination barrier could not be written.", e);
			}

			isTerminated = true;
		}
	}

	@Override
	public synchronized void process(WatchedEvent event) {

		if (event.getType() == Event.EventType.None || event.getState() == KeeperState.Expired) {
			// We need to reconnect
			this.connect();
		} else {
			for (TerminationWatcher watcher : this.watchers) {
				watcher.terminated();
			}
		}
	}

	/**
	 * Tell this termination monitor that there is a source running in the same VM that we have to wait for. The
	 * termination monitor will only set its state to "terminated" in Zookeeper, when all sources reported that all of
	 * their content has been processed by the system.
	 * 
	 * @param sourceId
	 *            the identifier for the source, we should wait for termination on.
	 */
	public void registerSource(String sourceId) {
		this.unfinishedSources.add(sourceId);
	}
}
