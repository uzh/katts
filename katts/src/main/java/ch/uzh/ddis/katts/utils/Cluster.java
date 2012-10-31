package ch.uzh.ddis.katts.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;

import backtype.storm.Config;

public final class Cluster {
	
	
	public static ZooKeeper createZooKeeper(Map conf) throws IOException {
		
		@SuppressWarnings("unchecked")
		List<String> zooKeeperServers = (List<String>)conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		
		Integer port = (Integer)conf.get(Config.STORM_ZOOKEEPER_PORT);
		
		
		StringBuilder connection = new StringBuilder();
		
		for (String server : zooKeeperServers) {
			connection.append(server).append(":").append(port).append(",");
		}
		
		return new ZooKeeper(connection.toString(), 3000, null);
	}

}
