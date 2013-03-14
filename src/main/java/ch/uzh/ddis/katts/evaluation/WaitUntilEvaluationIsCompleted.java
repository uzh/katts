package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

/**
 * This class implements a runnable to wait for the termination of the evaluation process.
 * 
 * @author Thomas Hunziker
 *
 */
public class WaitUntilEvaluationIsCompleted {

	public static void main(String args[]) throws IOException, KeeperException, InterruptedException {

		if (args.length <= 0) {
			System.out.println("Usage: WaitUntilEvaluationIsComplete ZooKeeperServer:ZooKeeperPort");
			System.exit(0);
		}

		String zooKeeperConnection = args[0];

		Waiter waiter = new Waiter(zooKeeperConnection, Aggregator.KATTS_EVALUATION_COMPLETED_ZK_PATH);

		System.out.println("Start watching the evaluation completion...");
		
		// Block here the thread, until it is finished
		waiter.run();
		
		System.out.println("Evaluation is completed.");

	}
}
