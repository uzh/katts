package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * This is the main class which, starts the evaluation process.
 * 
 * @author Thomas Hunziker
 *
 */
class Evaluation {
	
	public static void main(String[] args) throws IOException, TException {

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}
		
		String googleSpreadSheetName = null;
		String googleUsername = null;
		String googlePassword = null;
		String jobName = null;
		String zooKeeperServer = "localhost";
		String zooKeeperPort = "2181";
		String nimbusHost = "localhost";
		int thriftPort = 6627;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--google-spreadsheet-name")) {
				i++;
				googleSpreadSheetName = args[i];
			} else if (args[i].equalsIgnoreCase("--google-username")) {
				i++;
				googleUsername = args[i];
			} else if (args[i].equalsIgnoreCase("--google-password")) {
				i++;
				googlePassword = args[i];
			} else if (args[i].equalsIgnoreCase("--nimbus-host")) {
				i++;
				nimbusHost = args[i];
			} else if (args[i].equalsIgnoreCase("--thrift-port")) {
				i++;
				thriftPort = Integer.valueOf(args[i]);
			} else if (args[i].equalsIgnoreCase("--job-name")) {
				i++;
				jobName = args[i];
			} 
			else{
				i++;
				// Unknown parameter, ignore it.
			}
		}
		
		// Create Thrift Client
		TTransport transport = new TFramedTransport(new TSocket(nimbusHost, thriftPort));
		TProtocol protocol = new TBinaryProtocol(transport);
		transport.open();
		
		Aggregator aggregator = new Aggregator(protocol, jobName);
		
		if (googleSpreadSheetName != null && googleUsername != null && googlePassword != null) {
			GoogleSpreadsheet spreadsheet = new GoogleSpreadsheet(googleUsername, googlePassword, googleSpreadSheetName);
			aggregator.setGoogleSpreadsheet(spreadsheet);
		}

		aggregator.aggregateMessagePerHost();
		
		aggregator.sendFinishSignal();
		
	}
	
	
	public static String getUsageMessage() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("Usage: Evaluation").append("\n\n");
		return builder.toString();
	}
}
