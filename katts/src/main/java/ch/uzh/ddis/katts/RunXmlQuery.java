package ch.uzh.ddis.katts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.commons.io.FileUtils;

import ch.uzh.ddis.katts.monitoring.Recorder;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;
import ch.uzh.ddis.katts.spouts.file.FileTripleReader;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

public class RunXmlQuery {
	
	public static final String CONF_EVALUATION_FOLDER_NAME = "katts_evaluation_folder";

	public static void main(String[] args) {

		if (args.length == 0 || args[0] == null) {
			System.out.println(getUsageMessage());
			System.exit(0);
		}

		long monitoringRecordInterval = 15;
		boolean monitoring = true;
		String monitoringPath = "";
		String topologyName = "katts-topology";
		String path = null;
		String terminationCheckInterval = null;
		String terminationFilePath = null;
		String startingFilePath = null;
		String evaluationFolder = null;
		int numberOfProcessors = 10;
		int numberOfWorkers = 1000;
		float factorOfThreadsPerProcessor = 1.1f;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("--monitoring-record-interval")) {
				i++;
				monitoringRecordInterval = Long.valueOf(args[i]);
			} else if (args[i].equalsIgnoreCase("--monitoring")) {
				i++;
				monitoring = args[i].equals("0") ? false : true;
			} else if (args[i].equalsIgnoreCase("--monitoring-path")) {
				i++;
				monitoringPath = args[i];
			} else if (args[i].equalsIgnoreCase("--topology-name")) {
				i++;
				topologyName = args[i];
			} else if (args[i].equalsIgnoreCase("--termination-check-interval")) {
				i++;
				terminationCheckInterval = args[i];
			} else if (args[i].equalsIgnoreCase("--starting-file-path")) {
				i++;
				startingFilePath = args[i];
			} else if (args[i].equalsIgnoreCase("--termination-file-path")) {
				i++;
				terminationFilePath = args[i];
			} else if (args[i].equalsIgnoreCase("--evaluation-folder-path")) {
				i++;
				evaluationFolder = args[i];
			} else	if (args[i].equalsIgnoreCase("--number-of-processors")) {
				i++;
				numberOfProcessors = Integer.valueOf(args[i]);
			} else	if (args[i].equalsIgnoreCase("--number-of-workers")) {
				i++;
				numberOfWorkers = Integer.valueOf(args[i]);
			} else	if (args[i].equalsIgnoreCase("--factor-of-threads-per-processor")) {
				i++;
				factorOfThreadsPerProcessor = Float.valueOf(args[i]);
			} 
			else if (args[i].startsWith("--")) {
				i++;
				// Unknown parameter, ignore it.
			}
			else {
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
		TopologyBuilder builder = new TopologyBuilder();

		Config conf = new Config();
		conf.setNumWorkers(numberOfWorkers);

		// The max spout pending determines how many spout tuples can be pending. Pending means that the tuples is not
		// yet failed or acked.
		// TODO: Let this value be controlled over the configuration
		conf.setMaxSpoutPending(10000);

		if (terminationCheckInterval != null) {
			conf.put(TerminationMonitor.CONF_TERMINATION_CHECK_INTERVAL, terminationCheckInterval);
		}

		if (startingFilePath != null) {
			conf.put(FileTripleReader.CONF_STARTING_FILE_PATH_VAR_NAME, startingFilePath);
		}

		if (terminationFilePath != null) {
			conf.put(TerminationMonitor.CONF_TERMINATION_FILE_PATH, terminationFilePath);
		}
		
		if (evaluationFolder != null) {
			conf.put(CONF_EVALUATION_FOLDER_NAME, evaluationFolder);
		}

		if (monitoring) {

			List<String> hookClass = new ArrayList<String>();
			hookClass.add("ch.uzh.ddis.katts.monitoring.TaskMonitor");
			conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hookClass);

			conf.put(Recorder.MONITORING_FOLDER_PATH, monitoringPath);

			// Log every 15 seconds the Java Virtual Machine properties
			conf.put(VmMonitor.RECORD_INVERVAL, monitoringRecordInterval);
		}

		builder.setQuery(query);
		builder.setFactorOfThreadsPerProcessor(factorOfThreadsPerProcessor);
		builder.setParallelismByNumberOfWorkers(numberOfProcessors);
		
		// Write out some information about the job for evaluations purposes:
		if (evaluationFolder != null) {
			
			try {
				FileUtils.writeStringToFile(new File(evaluationFolder + "/number_of_processors"), Integer.toString(numberOfProcessors));
				FileUtils.writeStringToFile(new File(evaluationFolder + "/expected_number_of_tasks"), Long.toString(builder.getEstimatedNumberOfTasks()));
				FileUtils.writeStringToFile(new File(evaluationFolder + "/factor_of_threads_per_processor"), Float.toString(builder.getFactorOfThreadsPerProcessor()));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
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
