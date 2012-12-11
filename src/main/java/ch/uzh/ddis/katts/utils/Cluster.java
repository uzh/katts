package ch.uzh.ddis.katts.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import backtype.storm.Config;

/**
 * This util class helps to create a ZooKeeper instance direclty from a storm configuration.
 * 
 * @author Thomas Hunziker
 *
 */
public final class Cluster {

	public static ZooKeeper createZooKeeper(@SuppressWarnings("rawtypes") Map conf) throws IOException {

		@SuppressWarnings("unchecked")
		List<String> zooKeeperServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);

		// There is a strange behavior the port is sometimes a long and sometimes it is a integer. So we use object and
		// then use the toString method.
		Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);

		StringBuilder connection = new StringBuilder();

		for (String server : zooKeeperServers) {
			connection.append(server).append(":").append(port).append(",");
		}

		return new ZooKeeper(connection.toString(), 3000, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// Ignore. We need this only to prevent null pointer exceptions
			} });
	}

}
