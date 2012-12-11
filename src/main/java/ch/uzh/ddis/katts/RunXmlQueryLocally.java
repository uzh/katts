package ch.uzh.ddis.katts;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;

/**
 * This class runs the given query locally. The query must be serialized as XML.
 * 
 * @author Thomas Hunziker
 *
 */
public class RunXmlQueryLocally {
	
	public static final String RUN_TOPOLOGY_LOCALLY_CONFIG_KEY = "katts_run_topology_locally";
	
	/**
	 * This method builds the topology from the XML file. This method expected a XML file which contains the query
	 * serialized in XML.
	 * 
	 * @param args
	 */
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
