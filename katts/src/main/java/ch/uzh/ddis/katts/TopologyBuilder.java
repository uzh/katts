package ch.uzh.ddis.katts;

import org.apache.zookeeper.txn.CreateTxn;

import backtype.storm.generated.StormTopology;

import ch.uzh.ddis.katts.query.Node;
import ch.uzh.ddis.katts.query.Query;

public class TopologyBuilder extends backtype.storm.topology.TopologyBuilder{

	private Query query = null;
	
	private int parallelism = 10;
	
	/**
	 * This variable determines how many threads compared to workers
	 * are created. A value of 2.5 means that the system will adapt the
	 * parallelism in the way that in the end 2.5 more threads exists than
	 * workers. 
	 */
	public float factorOfThreadsPerProcessor = 1.1f;

	public Query getQuery() {
		return query;
	}

	public TopologyBuilder setQuery(Query query) {
		this.query = query;
		return this;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	/**
	 * This method sets the parallelism to a optimal value depending
	 * on the number of workers. The constant  factorOfThreadsPerProcessor
	 * controls the behavior of this method.
	 * 
	 * @param numberOfWorkers
	 */
	public void setParallelismByNumberOfWorkers(int numberOfWorkers) {
		
		int expectedNumberOfInfiniteParallelNodes = 0;
		int numberOfWorkersPretermined = 0;
		
		for (Node node : query.getNodes()) {
			int nodeParallelism = node.getParallelism();
			if (nodeParallelism < 1) {
				expectedNumberOfInfiniteParallelNodes++;
			}
			else {
				numberOfWorkersPretermined += nodeParallelism;
			}
		}
		
		float parallelism = (factorOfThreadsPerProcessor * (float)numberOfWorkers - numberOfWorkersPretermined) / expectedNumberOfInfiniteParallelNodes;
		
		this.setParallelism(Math.round(Math.max(parallelism, 1)));
	}
	
	
	public StormTopology createTopology() {
		
		
		for (Node node : query.getNodes()) {
			node.createTopology(this);
		}
		
		StormTopology topology = super.createTopology();
		
		// TODO: Optimize the resulting storm topology (e.g. MultiBolts)
		
		return topology;
	}

	public float getFactorOfThreadsPerProcessor() {
		return factorOfThreadsPerProcessor;
	}

	public void setFactorOfThreadsPerProcessor(float factorOfThreadsPerProcessor) {
		this.factorOfThreadsPerProcessor = factorOfThreadsPerProcessor;
	}
	
	
}
