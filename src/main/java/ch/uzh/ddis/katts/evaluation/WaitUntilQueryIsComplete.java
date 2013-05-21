package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import ch.uzh.ddis.katts.monitoring.TerminationMonitor;

/**
 * This class implements a runnable to wait for the termination of the query process.
 * 
 * @author Thomas Hunziker
 * 
 */
public class WaitUntilQueryIsComplete {

	public static void main(String args[]) throws IOException, KeeperException, InterruptedException {

		if (args.length <= 0) {
			System.out.println("Usage: WaitUntilQueryIsComplete ZooKeeperServer:ZooKeeperPort");
			System.exit(0);
		}

		String zooKeeperConnection = args[0];

		Waiter waiter = new Waiter(zooKeeperConnection, TerminationMonitor.END_TIME_ZK_PATH);

		System.out.println("Start watching the query completion...");

		// Block here the thread, until it is finished
		waiter.run();

		System.out.println("Query is completed.");

	}
}
