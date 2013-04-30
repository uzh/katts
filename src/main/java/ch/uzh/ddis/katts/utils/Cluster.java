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

/**
 * This util class helps to create a ZooKeeper instance direclty from a storm configuration.
 * 
 * @author Thomas Hunziker
 * 
 */
public final class Cluster {

	private final static Logger LOG = LoggerFactory.getLogger(Cluster.class);

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
