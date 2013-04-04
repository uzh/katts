package ch.uzh.ddis.katts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import ch.uzh.ddis.katts.monitoring.VmMonitor;
import ch.uzh.ddis.katts.query.Query;
import ch.uzh.ddis.katts.utils.EvalInfo;

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
		Properties kattsProperties; // the contents of the katts.properties file
		
		if (args.length == 0 || args[0] == null) {
			throw new Exception("The first parameter must be the path to the xml file with the query.");
		}

		String path = args[0];
		Query query = Query.createFromFile(path).validate().optimize();

		int numberOfWorkers = 1;

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

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
	}

}
