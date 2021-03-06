package ch.uzh.ddis.katts.query;

import java.io.Serializable;

import backtype.storm.topology.TopologyBuilder;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;

/**
 * A node is basically a small program, which process some date. It can produce or / and consumes data. This interface
 * defines the basic method for implementing a configuration class for such a node.
 * 
 * @see ConsumerNode
 * @see ProducerNode
 * 
 * @author Thomas Hunziker
 * 
 */
public interface Node extends Serializable {

	/**
	 * This method returns an ID for the node. This id is used to identify the different components in the topology. The
	 * id is also used to set the identifier in the storm topology.
	 * 
	 * @return Unique Id in the topology
	 */
	String getId();

	/**
	 * This method is called, to check if this node is configured well.
	 * 
	 * @return
	 * @throws InvalidNodeConfigurationException
	 */
	boolean validate() throws InvalidNodeConfigurationException;

	/**
	 * This method returns the parallelism for this node.
	 * <ul>
	 * <li>parallelism < 1: means that the node is parallelizable. The system determines by itself how much.</li>
	 * <li>parallelism = 1: means that the node is not parallelizable.</li>
	 * <li>parallelism > 1: means that the system will create the given number of nodes (bolts / spouts).</li>
	 * </ul>
	 * 
	 * @return
	 */
	int getParallelism();

	/**
	 * This method is called whenever the topology is build.
	 * 
	 * @param topology
	 */
	void createTopology(TopologyBuilder topology);
	
}
