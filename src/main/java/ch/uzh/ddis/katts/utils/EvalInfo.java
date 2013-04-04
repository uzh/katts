/**
 * 
 */
package ch.uzh.ddis.katts.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import backtype.storm.Config;

/**
 * This class takes care of collecting and storing runtime parameters and other information useful for evaluating the
 * system by first storing the values in the storm configuration context, persisting them in the zookeeper, and finally
 * reading them out of zookeeper when the aggregation of runtime statistics takes place.
 * 
 * <p/>
 * Typically the process of storing these values is as follows:
 * <ol>
 * <li>Values are stored in the storm configuration object using a name-prefix in order to prevent duplicate variable
 * names ({@link EvalInfo#storePrefixedVariable(Config, String, Object)}, {@link EvalInfo#PREFIX}).</li>
 * <li>The values are read out of the storm config and persisted in zookeeper when the topology gets deployed. You can
 * initiate this process by calling {@link #persistToZookeeper(Config, Zookeeper)}.</li>
 * <li>In the evaluation phase, the values in zookeeper are read out and returned as a map by the
 * {@link #retrieveInfoFromZookeeper(ZooKeeper)} method, for further processing - for example for storage in a google
 * spreadsheet.</li>
 * </ol>
 * 
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public final class EvalInfo {

	/** This prefix will be added to all variable names by the #getPrefixedName() method. */
	public static final String PREFIX = "katts-eval-info.";

	/** This path will be used to store the configuration variables in Zookeeper. */
	public static final String ZK_PATH = "/katts-eval-info";

	/** Utility classes must not be instantated. */
	private EvalInfo() {
	}

	/**
	 * This method stores <code>value</code> into the supplied storm configuration object using a name that has been
	 * prefixed with {@link #PREFIX}.
	 * 
	 * @param stormConfig
	 *            the storm configuration object in which you want to store your variable.
	 * @param originalVariableName
	 *            the original name of the variable without the prefix. The prefix will be prepended by this method.
	 * @param value
	 *            the object that should be stored in the configuration.
	 * @return the previous value associated with key, or null if there was no mapping for key. (A null return can also
	 *         indicate that the map previously associated null with key.)
	 */
	public static Object storePrefixedVariable(Config stormConfig, String originalVariableName, String value) {
		return stormConfig.put(PREFIX + originalVariableName, value);
	}

	/**
	 * This method reads out all configuration parameters from <code>stormConfig</code> that have the prefix
	 * {@link #PREFIX} and stores these values into Zookeeper using the the supplied zookeeper client connection. In
	 * this process the prefix will be removed from the variable names.
	 * 
	 * @param stormConfig
	 *            the configuration object to search prefixed parameters in.
	 * @param zkClient
	 *            the connection object to use for the connection to Zookeeper.
	 * @return
	 * @throws KeeperException
	 *             if the server returns a non-zero error code
	 * @throws InterruptedException
	 *             if the transaction is interrupted
	 */
	public static void persistInfoToZookeeper(Map<Object, Object> stormConfig, ZooKeeper zkClient)
			throws KeeperException, InterruptedException {
		// create root node
		zkClient.create(ZK_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		// create node for each item of the map
		for (Object keyObj : stormConfig.keySet()) {
			String key = keyObj.toString();
			if (key.startsWith(PREFIX)) {
				String keyWithoutPrefix = key.substring(PREFIX.length());
				zkClient.create(ZK_PATH + "/" + keyWithoutPrefix, stormConfig.get(key).toString().getBytes(),
						Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		}
	}

	/**
	 * This method reads out all evaluation information from zookeeper and returns it as a map.
	 * 
	 * @param zkClient
	 *            the connection object to use for the connection to Zookeeper.
	 * @return the map containing all the evaluation information that has been stored in Zookeeper.
	 * @throws InterruptedException
	 *             If the server transaction is interrupted.
	 * @throws KeeperException
	 *             If the server signals an error with a non-zero error code.
	 */
	public static Map<String, String> retrieveInfoFromZookeeper(ZooKeeper zkClient) throws KeeperException,
			InterruptedException {
		Map<String, String> result = new HashMap<String, String>();

		for (String child : zkClient.getChildren(ZK_PATH, false)) {
			// the child variable contains only the name of the child and not its full path
			result.put(child, new String(zkClient.getData(ZK_PATH + "/" + child, null, null)));
		}

		return result;
	}

}
