package ch.uzh.ddis.katts.query;

import java.util.List;

import backtype.storm.topology.IRichBolt;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * A consumer node eats one or more streams from another node. In context 
 * of storm a consumer node must be always a Bolt.
 * 
 * @author Thomas Hunziker
 *
 */
public interface ConsumerNode extends Node{

	/**
	 * Returns a list of StreamConsumers. A StreamConsumers
	 * refers to a stream and a grouping on this stream.
	 * 
	 * @see StreamConsumer
	 * 
	 * @return
	 */
	public List<StreamConsumer> getConsumers();
	
	/**
	 * Returns the bolt, which processes the 
	 * data of this kind of node.
	 * 
	 * @see Bolt
	 * 
	 * @return
	 */
	public Bolt getBolt();

}
