package ch.uzh.ddis.katts.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;

/**
 * This util class helps to create a ZooKeeper instance direclty from a storm configuration.
 * 
 * @author Thomas Hunziker
 * 
 */
public final class Cluster {

	private final static Logger LOG = LoggerFactory.getLogger(Cluster.class);

	/**
	 * According to https://github.com/Netflix/curator/wiki/Framework we should only have one curator per VM, so we keep
	 * a static reference to it here.
	 */
	private static CuratorFramework curator;

	/**
	 * This method creates the connection string to connect to the Zookeeper server.
	 * 
	 * @param conf
	 *            the storm configuration object containing the information needed.
	 * @return the connectio string that can be used to create an object of type {@link ZooKeeper}.
	 */
	public static String createZookeeperConnectionString(@SuppressWarnings("rawtypes") Map conf) {
		@SuppressWarnings("unchecked")
		List<String> zooKeeperServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);

		// There is a strange behavior the port is sometimes a long and sometimes it is a integer. So we use object and
		// then use the toString method.
		Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);

		StringBuilder connection = new StringBuilder();

		for (String server : zooKeeperServers) {
			connection.append(server).append(":").append(port).append(",");
		}

		return connection.toString();
	}

	public static ZooKeeper createZooKeeper(@SuppressWarnings("rawtypes") Map conf) throws IOException {
		return new ZooKeeper(createZookeeperConnectionString(conf), 30 * 1000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// Ignore. We need this only to prevent null pointer exceptions
			}
		});
	}

	/**
	 * Returns (or creates if necessary) a curator instance that simplifies access to zookeeper. The retry policy of
	 * this method is to try at most 10 times during 30 seconds to connect before failing.
	 * 
	 * @param conf
	 *            the storm configuration object that contains the informatino to connect to zookeeper.
	 * @throws IOException
	 *             in case of a ZK error.
	 */
	public synchronized static CuratorFramework getCuratorClient(@SuppressWarnings("rawtypes") Map conf)
			throws IOException {
		if (curator == null) {
			curator = CuratorFrameworkFactory.newClient(createZookeeperConnectionString(conf), new RetryPolicy() {

				@Override
				public boolean allowRetry(int retryCount, long elapsedTimeMs) {
					return retryCount < 10 || elapsedTimeMs < 30 * 1000; // try 10 times during at max 30 seconds
				}
			});
			curator.start();
			// according to the docs, we should actually also close the connection...
		}
		return curator;
	}

	/**
	 * This method returns the nodes host on which this code is executed on.
	 * 
	 * @return
	 */
	public static String getHostIdentifier() {
		String result = null;

		try {
			result = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			LOG.warn("Can't get hostname. Instead the IP address is used.");
		}

		if (result == null) {
			try {
				result = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				LOG.warn("Can't get ip address. Instead a random number is used.");
			}
		}

		if (result == null) {
			LOG.warn("Could not retrieve neither the hostname nor the ip address of ip. Using random number as host"
					+ "identifier.");
			SecureRandom random = new SecureRandom();
			result = Long.toString(random.nextLong());
		}

		return result;
	}
}
