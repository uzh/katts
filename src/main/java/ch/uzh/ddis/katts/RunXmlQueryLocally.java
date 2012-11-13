package ch.uzh.ddis.katts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.monitoring.Recorder;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;

public class RunXmlQueryLocally {
	
	public static final String RUN_TOPOLOGY_LOCALLY_CONFIG_KEY = "katts_run_topology_locally";
	
	
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0 || args[0] == null) {
			throw new Exception("The first parameter must be the path to the xml file with the query.");
		}
		
		String path = args[0];
		Query query = Query.createFromFile(path).validate().optimize();
		
		int numberOfWorkers = 20;
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(numberOfWorkers);
		
		TopologyBuilder builder = new TopologyBuilder(conf);
		builder.setQuery(query);
		builder.setParallelismByNumberOfProcessors(numberOfWorkers);
		
		
		List<String> hookClass = new ArrayList<String>();
		hookClass.add("ch.uzh.ddis.katts.monitoring.TaskMonitor");
		conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hookClass);
		
		conf.put(RUN_TOPOLOGY_LOCALLY_CONFIG_KEY, true);
		
		// Log every 15 seconds the Java Virtual Machine properties
		conf.put(VmMonitor.RECORD_INVERVAL, 30);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		
		
		
		
	}

}
