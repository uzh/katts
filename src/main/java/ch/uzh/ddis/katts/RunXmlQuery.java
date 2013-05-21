package ch.uzh.ddis.katts;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.bind.JAXBException;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;
import ch.uzh.ddis.katts.query.Node;
import ch.uzh.ddis.katts.query.Query;
import ch.uzh.ddis.katts.utils.EvalInfo;

/**
 * This is the main class from which a query is executed from. This class is called by Storm to build the topology.
 * 
 * @author Thomas Hunziker
 * 
 */
public class RunXmlQuery {

	public static final String CONF_EVALUATION_FOLDER_NAME = "katts_evaluation_folder";

	/**
	 * This method builds the topology from the XML file. This method expected a XML file which contains the query
	 * serialized in XML.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Properties kattsProperties; // the contents of the katts.properties file

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}

		// Read in the args
		boolean monitoring = true;
		String topologyName = "katts-topology";
		String path = null;
		String terminationCheckInterval = null;
		String evalNotes = "";
		long maxSpoutPending = 5 * 1000; // default is 5k
		int numberOfWorkers = 1000;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--monitoring")) {
				i++;
				monitoring = args[i].equals("0") ? false : true;
			} else if (args[i].equalsIgnoreCase("--topology-name")) {
				i++;
				topologyName = args[i];
			} else if (args[i].equalsIgnoreCase("--termination-check-interval")) {
				i++;
				terminationCheckInterval = args[i];
			} else if (args[i].equalsIgnoreCase("--eval-notes")) {
				i++;
				evalNotes = args[i];
			} else if (args[i].equalsIgnoreCase("--max-spout-pending")) {
				i++;
				maxSpoutPending = Long.parseLong(args[i]);
			} else if (args[i].equalsIgnoreCase("--number-of-workers")) {
				i++;
				numberOfWorkers = Integer.valueOf(args[i]);
			} else if (args[i].startsWith("--")) {
				i++;
				// Unknown parameter, ignore it.
			} else {
				path = args[i];
			}
		}

		if (path == null) {
			System.out.println("You need to specify a file with the query.");
			getUsageMessage();
			System.exit(0);
		}

		Query query = null;
		try {
			query = Query.createFromFile(path).validate().optimize();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (JAXBException e) {
			e.printStackTrace();
			System.exit(0);
		}

		Config conf = new Config();
		conf.setNumWorkers(numberOfWorkers);
		
		// read properties file
		kattsProperties = new Properties();
		try {
			kattsProperties.load(RunXmlQuery.class.getResourceAsStream("/katts.properties"));
		} catch (IOException e1) {
			e1.printStackTrace();
			System.exit(0); // if we can't read the properties file, something must be wrong with the build
		}

		// add some information values to the config that are useful for the evaluation of the system
		EvalInfo.storePrefixedVariable(conf, "katts-version", kattsProperties.get("katts-version").toString());
		EvalInfo.storePrefixedVariable(conf, "eval-notes", evalNotes);

		// The max spout pending determines how many spout tuples can be pending. Pending means that the tuples is not
		// yet failed or acked.
		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 10); // 10 acker threads let's see what this brings
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60 * 60); // we wait for up to one hour
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, maxSpoutPending);

		if (terminationCheckInterval != null) {
			conf.put(TerminationMonitor.CONF_TERMINATION_CHECK_INTERVAL, terminationCheckInterval);
		}

		if (monitoring) {
			List<String> hookClass = new ArrayList<String>();
			hookClass.add("ch.uzh.ddis.katts.monitoring.TaskMonitor");
			conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hookClass);
		}

		// Disable reliability TODO lorenz: do I have to set this to some value?
		// conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

		TopologyBuilder builder = new TopologyBuilder();
		for (Node node : query.getNodes()) {
			node.createTopology(builder);
		}

		try {
			StormTopology topology = builder.createTopology();
			StormSubmitter.submitTopology(topologyName, conf, topology);
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
			System.exit(0);
		}

	}

	private static String getUsageMessage() {
		StringBuilder builder = new StringBuilder();

		builder.append("Usage: RunXmlQuery [option...] path-to-file-with-xml-query.xml").append("\n\n");
		builder.append("Options: --name value").append("\n\n");
		builder.append("monitoring-record-interval: The time between the vm data is recorded in seconds. Default: 15")
				.append("\n");
		builder.append(
				"monitoring: Either 0 or 1, whereby 0 deactivates the monitoring and 1 activates the monitoring. Default: 0")
				.append("\n");
		builder.append(
				"monitoring-path: The path to a folder in which the monitoring data is stored. This folder must exists on each node in the cluster.")
				.append("\n");
		builder.append("topology-name: This is the name of the topology.").append("\n");

		return builder.toString();
	}

}
