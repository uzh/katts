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
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class TerminationMonitor {

	/** The singleton instance. Each worker can only have one termiation monitor. */
	private static TerminationMonitor instance;

	/**
	 * A reference to the storm configuration object. The information in this object is necessary to start a connection
	 * to Zookeeper.
	 */
	private Map<?, ?> stormConf;

	/** A flag to remember if this monitor has "told" the Zookeeper that all its sources have finished processing. */
	private boolean isTerminated = false;

	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";
	public static final String KATTS_TERMINATION_ZK_PATH = "/katts_terminated";
	public static final String KATTS_TUPLES_OUTPUTTED_ZK_PATH = "/katts_number_of_tuples_outputed";

	/**
	 * This list contains a reference to all objects that need to be informed when this worker has finished processing
	 * input.
	 */
	private List<TerminationCallback> terminationCallbacks = new ArrayList<TerminationCallback>();

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

	private TerminationMonitor(Map<?, ?> stormConf) {
		this.stormConf = stormConf;
	}

	public static synchronized TerminationMonitor getInstance(Map<?, ?> stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	/**
	 * Registers a callback object with this monitor. As soon as the last source has finished processing its input all
	 * registered callbacks will be informed about the fact that this worker instance has finished processing and will
	 * now temrinate.
	 * 
	 * @param callback
	 *            the callback object that should be informed when the processing has finished.
	 */
	public synchronized void registerTerminationCallback(TerminationCallback callback) {
		this.terminationCallbacks.add(callback);
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
			ZooKeeper zooKeeper;

			logger.info("Run terminated.");

			// inform all TerminationCallbacks
			synchronized (this.terminationCallbacks) { // no new callbacks can be added
				for (TerminationCallback callback : this.terminationCallbacks) {
					callback.workerTerminated();
				}
			}

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

	/**
	 * Classes that implement this interface can register themselves with the {@link TerminationMonitor} using the
	 * {@link TerminationMonitor#registerTerminationCallback(TerminationCallback)} method. When this monitor has has
	 * been informed by all registered sources that thei have exhausted their input using the
	 * {@link TerminationMonitor#terminate(String)} method, all registered {@link TerminationCallback} objects will have
	 * their {@link #workerTerminated()} method called before this monitor informs the other workers by writing the
	 * finished flag into zookeeper.
	 * 
	 * The {@link #workerTerminated()} method can be used to do cleanup work such as writing performance statistics to
	 * the filesystem or into zookeeper.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 * 
	 */
	public static interface TerminationCallback {

		/**
		 * This method will be called when all sources that have registered themselves with the
		 * {@link TerminationMonitor} have signalled that they have finished processing input.
		 */
		public void workerTerminated();
	}
}
