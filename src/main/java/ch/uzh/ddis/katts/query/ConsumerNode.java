package ch.uzh.ddis.katts.query;

import java.util.List;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * A consumer node eats one or more streams from another node. In context of storm a consumer node must be always a
 * Bolt.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface ConsumerNode extends Node {

	/**
	 * Returns a list of StreamConsumers. A StreamConsumers refers to a stream and a grouping on this stream.
	 * 
	 * @see StreamConsumer
	 * 
	 * @return
	 */
	public List<StreamConsumer> getConsumers();

	public void appendConsumer(StreamConsumer consumer);

	
	/**
	 * This method creates an instance of the bolt class this configuration object is holding the configuration
	 * information for. A new instance will be returned each time this method is called.
	 * 
	 * @see Bolt
	 * 
	 * @return an instance of this bolt.
	 */
	public Bolt createBoltInstance();

}
