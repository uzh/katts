package ch.uzh.ddis.katts.monitoring;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import backtype.storm.Config;

import ch.uzh.ddis.katts.RunXmlQuery;
import ch.uzh.ddis.katts.utils.Cluster;

public class TerminationMonitor {

	private Thread runner;

	private static TerminationMonitor instance;

	private boolean isDataSendToOutput = false;
	private boolean isAnyDataSend = false;

	private int terminationCheckInterval = 20000;

	private long lastOutputSendOn = 0;

	private long messageCount = 0;

	private ZooKeeper zooKeeper;

	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";
	public static final String KATTS_TERMINATION_ZK_PATH = "/katts_terminated";
	public static final String KATTS_TUPLES_OUTPUTTED_ZK_PATH = "/katts_number_of_tuples_outputed";

	private TerminationMonitor(Map stormConf) {

		if (stormConf.containsKey(CONF_TERMINATION_CHECK_INTERVAL)) {
			terminationCheckInterval = Integer.valueOf((String) stormConf.get(CONF_TERMINATION_CHECK_INTERVAL));
		}

		if (terminationCheckInterval > 0) {
			try {
				zooKeeper = Cluster.createZooKeeper(stormConf);
			} catch (Exception e) {
				throw new RuntimeException("Can't create termination ZooKeeper watcher.", e);
			}

			WaitMonitor waiter = new WaitMonitor(this);

			runner = new Thread(waiter);
			runner.start();
		}

	}

	public static TerminationMonitor getInstance(Map stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	public synchronized void addTerminationWatcher(Watcher watcher) {
		try {
			zooKeeper.exists(KATTS_TERMINATION_ZK_PATH, watcher);
		} catch (KeeperException e) {
			throw new RuntimeException("Could not add watcher on the termination znode on ZooKeeper.", e);
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not add watcher on the termination znode on ZooKeeper.", e);
		}
	}

	public synchronized void dataIsSendToOutput() {
		isAnyDataSend = true;
		isDataSendToOutput = true;
		lastOutputSendOn = System.currentTimeMillis();
		messageCount++;

	}

	private class WaitMonitor implements Runnable {

		TerminationMonitor monitor;
		boolean isStopped = false;

		WaitMonitor(TerminationMonitor monitor) {
			this.monitor = monitor;
		}

		@Override
		public void run() {

			while (!isStopped) {
				monitor.isDataSendToOutput = false;
				try {
					Thread.sleep(monitor.terminationCheckInterval);
				} catch (InterruptedException e) {
					throw new RuntimeException(
							"The termination monitor could not send the monitor thread to background.", e);
				}

				// When the flag is not changed, then we know that in the measured interval
				// no data is send to output. The isAnyDataSend indicates that some data was send. If this flag is not
				// set this VM does probably do not produce any output and should not send the termination signal.
				if (!monitor.isDataSendToOutput && isAnyDataSend) {
					isStopped = true;

					try {
						monitor.zooKeeper.create(KATTS_TERMINATION_ZK_PATH, Long.toString(monitor.lastOutputSendOn)
								.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch (Exception e) {
						throw new RuntimeException("The termination barrier could not be written.", e);
					}

					try {
						monitor.zooKeeper.create(KATTS_TUPLES_OUTPUTTED_ZK_PATH, Long.toString(monitor.messageCount)
								.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					} catch (Exception e) {
						throw new RuntimeException("The termination barrier could not be written.", e);
					}

				}
			}
		}
	}

}
