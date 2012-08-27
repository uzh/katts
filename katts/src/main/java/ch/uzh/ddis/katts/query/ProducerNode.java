package ch.uzh.ddis.katts.query;

import java.util.List;

import ch.uzh.ddis.katts.query.stream.Stream;

/**
 * The ProducerNode emits some data on one or more stream. It can 
 * be either a Bolt or a Spout in Context of Storm.
 * 
 * @author Thomas Hunziker
 *
 */
public interface ProducerNode extends Node{
	
	/**
	 * Returns a list of streams produces by this node.
	 * 
	 * @return List of Streams
	 */
	public List<Stream> getProducers();

}
