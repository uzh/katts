package ch.uzh.ddis.katts.monitoring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.utils.Cluster;
import ch.uzh.ddis.katts.utils.EvalInfo;

import com.netflix.curator.framework.CuratorFramework;

/**
 * This monitor checks when the query is terminated and informs external processes about query termination by writing
 * the termination time of this worker into Zookeeper. The process is as follows: Each worker (each vm) writes its
 * system time into a zookeeper node as soon as it has "started" processing. When it has processed all its input it
 * writes its curren system time into start time
 * 
 * 
 * @see Termination
 * 
 * @author Thomas Hunziker
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class TerminationMonitor {

	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";

	/** Each storm worker (vm) writes its system time into this path as soon as it has started processing input. */
	public static final String HOST_START_TIMES_ZK_PATH = "/katts/start_times";

	/**
	 * Each storm worker (vm) writes its system time into this path as a childnode, as soon as it has finished
	 * processing all its input. When all workers that have "registered" themselves by setting their start time in
	 * {@link #HOST_START_TIMES_ZK_PATH} have a corresponding entry in {@link #HOST_END_TIMES_ZK_PATH} the computation
	 * has finished and the runtime can be computed by taking the maximum end_time minus the minimum start_time.
	 */
	public static final String HOST_END_TIMES_ZK_PATH = "/katts/end_times";

	/** The minimum start time of all hosts will be written into the node at this path. */
	public static final String START_TIME_ZK_PATH = "/katts/start_time";

	/** The maximum end time of all hosts will be written into the node at this path. */
	public static final String END_TIME_ZK_PATH = "/katts/end_time";

	// public static final String KATTS_TUPLES_OUTPUTTED_ZK_PATH = "/katts_number_of_tuples_outputed";

	/** The singleton instance. Each worker can only have one termiation monitor. */
	private static TerminationMonitor instance;

	/**
	 * This method returns the singleton object if it already exists and creates it, in the latter case. This method is
	 * synchronized and can be called from multiple threads at the same time.
	 * 
	 * @param stormConf
	 *            the storm configuration object containing the connectin information for Zookeeper.
	 * @return the monitor singleton for this VM.
	 */
	public static synchronized TerminationMonitor getInstance(Map<?, ?> stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	/**
	 * A reference to the storm configuration object. The information in this object is necessary to start a connection
	 * to Zookeeper.
	 */
	private Map<?, ?> stormConfiguration;

	/**
	 * We use this curator instance to talk to ZK. According to https://github.com/Netflix/curator/wiki/Framework we
	 * should only have one curator instance and reuse it throughout the VM.
	 */
	private CuratorFramework curator;

	/** A flag to remember if this monitor has "told" the Zookeeper that all its sources have finished processing. */
	private boolean endTimeSet = false;

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

	/**
	 * This object is used to guarantee that the start time can only be set once and that it is set only by the first
	 * call of {@link #start()}.
	 */
	private boolean startTimeSet = false;

	/**
	 * This string uniquely identifies this worker node inside the storm instance and it is used to monitor the start
	 * and the stop time of the system.
	 */
	private String stormWorkerIdentifier;

	/**
	 * Singleton classes need private constructors.
	 * 
	 * @param stormConf
	 *            the map containing the storm configuration, which is necessary to create the connection to Zookeeper.
	 */
	private TerminationMonitor(Map<?, ?> stormConf) {
		this.stormConfiguration = stormConf;

		/*
		 * TODO: this only works for as long as we have ALWAYS only one storm worker per host.
		 */
		this.stormWorkerIdentifier = Cluster.getHostIdentifier();

		try {
			this.curator = Cluster.getCuratorClient(this.stormConfiguration);
		} catch (IOException e) {
			// we should stop everything right here, since this is not going to end up well
			throw new IllegalStateException("Could not create the Zookeeper connection using the Curator.", e);
		}

		// write evaluation info from storm configuration into zookeeper
		try {
			EvalInfo.persistInfoToZookeeper(stormConf, this.curator);
		} catch (Exception e) {
			this.logger.warn("Could not store configuration information to zookeper. "
					+ "Probably it already existed. Exception was: " + e.getMessage());
		}

		// create root node for the end times
		try {
			if (this.curator.checkExists().forPath(HOST_END_TIMES_ZK_PATH) == null) {
				this.curator.create().creatingParentsIfNeeded().forPath(HOST_END_TIMES_ZK_PATH);
			}
		} catch (Exception e) {
			throw new RuntimeException("Can't create node for path '" + HOST_END_TIMES_ZK_PATH + "' because: "
					+ e.getMessage(), e);
		}

		/*
		 * Register a watcher to the end times path, so we will be informed as soon as all workers have finished
		 * processing their input.
		 */
		Watcher terminationWatcher = new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				CuratorFramework ctr;
				int startTimeCount;
				int endTimeCount;

				try {
					List<String> starTimePaths;
					List<String> endTimePaths;

					ctr = TerminationMonitor.this.curator;
					starTimePaths = ctr.getChildren().forPath(HOST_START_TIMES_ZK_PATH);
					endTimePaths = ctr.getChildren().forPath(HOST_END_TIMES_ZK_PATH);

					startTimeCount = starTimePaths.size();
					endTimeCount = endTimePaths.size();

					if (startTimeCount > 0 && startTimeCount == endTimeCount) {

						// First: inform all TerminationCallbacks in local VM
						synchronized (TerminationMonitor.this.terminationCallbacks) { // no new callbacks can be added
							for (TerminationCallback callback : TerminationMonitor.this.terminationCallbacks) {
								callback.workerTerminated();
							}
						}

						// Second: Compute global start and end time

						Long minimumStartTime = Long.MAX_VALUE;
						Long maximumEndTime = Long.MIN_VALUE;

						// find minimum start time
						for (String startTimePath : starTimePaths) {
							byte[] data = ctr.getData().forPath(HOST_START_TIMES_ZK_PATH + "/" + startTimePath);
							Long currentStartTime = (Long) SerializationUtils.deserialize(data);
							if (currentStartTime < minimumStartTime) {
								minimumStartTime = currentStartTime;
							}
						}

						// find maximum end time
						for (String endTimePath : endTimePaths) {
							byte[] data = ctr.getData().forPath(HOST_END_TIMES_ZK_PATH + "/" + endTimePath);
							Long currentStartTime = (Long) SerializationUtils.deserialize(data);
							if (currentStartTime > maximumEndTime) {
								maximumEndTime = currentStartTime;
							}
						}

						/*
						 * write start and end time back into ZK, this call will be tried by all worker nodes. therefore
						 * we will need to check if the node already exists.
						 */
						if (ctr.checkExists().forPath(START_TIME_ZK_PATH) == null) {
							ctr.create().forPath(START_TIME_ZK_PATH, SerializationUtils.serialize(minimumStartTime));
						}
						if (ctr.checkExists().forPath(END_TIME_ZK_PATH) == null) {
							ctr.create().forPath(END_TIME_ZK_PATH, SerializationUtils.serialize(maximumEndTime));
						}

					}

					ctr.getChildren().usingWatcher(this).forPath(HOST_END_TIMES_ZK_PATH);
				} catch (Exception e) {
					throw new IllegalStateException("Error while checking for query completion", e);
				}
			}

		};

		// add the watcher to the path of the end time
		try {
			this.curator.getChildren().usingWatcher(terminationWatcher).forPath(HOST_END_TIMES_ZK_PATH);
		} catch (Exception e) {
			throw new IllegalStateException("Could not add watcher the Zookeeper connection using the Curator.", e);
		}
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

		if (this.unfinishedSources.isEmpty()) {
			logger.info("Source with id " + sourceId + " is done processing. Run complete!");
		} else {
			logger.info("Source with id " + sourceId + " is done processing. Still waiting for others: "
					+ this.unfinishedSources);
		}

		/*
		 * Inform interested parties that this worker is done processing data as soon as we have no unfinished sources
		 * left.
		 */
		if (!this.endTimeSet && this.unfinishedSources.isEmpty()) {

			String endTimePath = HOST_END_TIMES_ZK_PATH + "/" + this.stormWorkerIdentifier;
			byte[] serializedTime = SerializationUtils.serialize(Long.valueOf(System.currentTimeMillis()));
			try {
				this.curator.create().creatingParentsIfNeeded().forPath(endTimePath, serializedTime);
			} catch (Exception e) {
				throw new RuntimeException("Can't write end time to ZK at path '" + endTimePath + "' because: "
						+ e.getMessage(), e);
			}

			endTimeSet = true;
		}
	}

	/**
	 * This method will set the start time for this worker in Zookeeper to the current system time to signal that the
	 * storm worker this VM is running in has started processing input. We only register the first call to this method.
	 * All following calls have no impact.
	 */
	public synchronized void start() {
		if (this.startTimeSet == false) { // the value has been false before
			String startTimePath = HOST_START_TIMES_ZK_PATH + "/" + this.stormWorkerIdentifier;
			byte[] serializedTime = SerializationUtils.serialize(Long.valueOf(System.currentTimeMillis()));
			try {
				this.curator.create().creatingParentsIfNeeded().forPath(startTimePath, serializedTime);
			} catch (Exception e) {
				throw new RuntimeException("Can't write start time to ZK at path '" + startTimePath + "' because: "
						+ e.getMessage(), e);
			}
			
			this.startTimeSet = true;
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
