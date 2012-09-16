package ch.uzh.ddis.katts;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import ch.uzh.ddis.katts.monitoring.Recorder;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class RunXmlQuery {
	
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
		
		for(int i = 0; i < args.length; i++){
			if(args[i].equalsIgnoreCase("--monitoring-record-interval")) {
				i++;
				monitoringRecordInterval = Long.valueOf(args[i]);
			}
			else if(args[i].equalsIgnoreCase("--monitoring")) {
				i++;
				monitoring = args[i].equals("0") ? false : true;
			}
			else if(args[i].equalsIgnoreCase("--monitoring-path")) {
				i++;
				monitoringPath = args[i];
			}
			else if(args[i].equalsIgnoreCase("--topology-name")) {
				i++;
				topologyName = args[i];
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
		int numberOfWorkers = 20;
		
		// TODO: Expose some important configurations to the user (topologyName, numerOfWorkers, etc.)
		Config conf = new Config();
		conf.setNumWorkers(numberOfWorkers);
		conf.setMaxSpoutPending(5000);
		
		if (monitoring) {
			
			List<String> hookClass = new ArrayList<String>();
			hookClass.add("ch.uzh.ddis.katts.monitoring.TaskMonitor");
			conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hookClass);
			
			conf.put(Recorder.MONITORING_FOLDER_PATH, monitoringPath);
			
			// Log every 15 seconds the Java Virtual Machine properties
			conf.put(VmMonitor.RECORD_INVERVAL, monitoringRecordInterval);
		}
		
		builder.setQuery(query);
		builder.setParallelismByNumberOfWorkers(numberOfWorkers);
		
		
		try {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
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
		builder.append("monitoring-record-interval: The time between the vm data is recorded in seconds. Default: 15").append("\n");
		builder.append("monitoring: Either 0 or 1, whereby 0 deactivates the monitoring and 1 activates the monitoring. Default: 0").append("\n");
		builder.append("monitoring-path: The path to a folder in which the monitoring data is stored. This folder must exists on each node in the cluster.").append("\n");
		builder.append("topology-name: This is the name of the topology.").append("\n");
		
		return builder.toString();
	}

}
