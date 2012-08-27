package ch.uzh.ddis.katts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.query.Query;

public class RunXmlQueryLocally {
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0 || args[0] == null) {
			throw new Exception("The first parameter must be the path to the xml file with the query.");
		}
		
		String path = args[0];
		Query query = Query.createFromFile(path).validate().optimize();
		TopologyBuilder builder = new TopologyBuilder();
		int numberOfWorkers = 20;
		
		builder.setQuery(query);
		builder.setParallelismByNumberOfWorkers(numberOfWorkers);
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(numberOfWorkers);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
	}

}
