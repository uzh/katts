package ch.uzh.ddis.katts;

import ch.uzh.ddis.katts.query.Query;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;

public class RunXmlQuery {
	public static void main(String[] args) throws Exception {
		
		if (args.length == 0 || args[0] == null) {
			throw new Exception("The first parameter must be the path to the xml file with the query.");
		}
		
		
		String path = args[0];
		Query query = Query.createFromFile(path).validate().optimize();
		TopologyBuilder builder = new TopologyBuilder();
		int numberOfWorkers = 20;
		
		// TODO: Expose some important configurations to the user (topologyName, numerOfWorkers, etc.)
		Config conf = new Config();
		conf.setNumWorkers(numberOfWorkers);
		conf.setMaxSpoutPending(5000);
		
		
		builder.setQuery(query);
		builder.setParallelismByNumberOfWorkers(numberOfWorkers);
		
		
		StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());		
		
	}

}
