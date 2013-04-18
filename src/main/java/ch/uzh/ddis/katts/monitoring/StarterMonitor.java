package ch.uzh.ddis.katts.monitoring;

import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.uzh.ddis.katts.RunXmlQueryLocally;
import ch.uzh.ddis.katts.utils.Cluster;
import ch.uzh.ddis.katts.utils.EvalInfo;

/**
 * This monitor is used to record, when the query execution starts. The start is identified by the first file reader
 * start reading.
 * 
 * @author Thomas Hunziker
 * 
 */
public class StarterMonitor {

	private static StarterMonitor instance;
	@SuppressWarnings("rawtypes")
	private Map stormConfiguration;
	private ZooKeeper zooKeeper;

	public static final String KATTS_STARTING_TIME_ZK_PATH = "/katts_starting_time";
	private Logger logger = LoggerFactory.getLogger(StarterMonitor.class);

	@SuppressWarnings("unchecked")
	// for some reason the storm api uses untyped maps
	private StarterMonitor(@SuppressWarnings("rawtypes") Map stormConf) {
		stormConfiguration = stormConf;

		try {
			zooKeeper = Cluster.createZooKeeper(stormConfiguration);
		} catch (IOException e) {
			throw new RuntimeException("Can't create ZooKeeper instance for monitoring the start time.", e);
		}

		// write evaluation info from storm configuration into zookeeper
		try {
			EvalInfo.persistInfoToZookeeper((Map<Object, Object>) stormConf, zooKeeper);
		} catch (Exception e) {
			this.logger.warn("Could not store configuration information to zookeper. "
					+ "Probably it already existed. Exception was: " + e.getMessage());
		}
	}

	@SuppressWarnings("rawtypes")
	public static StarterMonitor getInstance(Map stormConf) {
		if (instance == null) {
			instance = new StarterMonitor(stormConf);
		}

		return instance;
	}

	public void start() {

		if (!(Boolean) stormConfiguration.get(RunXmlQueryLocally.RUN_TOPOLOGY_LOCALLY_CONFIG_KEY)) {

			try {
				zooKeeper.create(KATTS_STARTING_TIME_ZK_PATH, Long.toString(System.currentTimeMillis()).getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				if (e.code().equals(KeeperException.Code.NODEEXISTS)) {
					logger.info("The starting time entry was set already by another instance.");
				} else {
					throw new RuntimeException("Can't create the starting time ZooKeeper entry.", e);
				}

			} catch (InterruptedException e) {
				throw new RuntimeException(
						"Can't create the starting time ZooKeeper entry, because the thread was interrupted.", e);
			}

		}
	}

}
