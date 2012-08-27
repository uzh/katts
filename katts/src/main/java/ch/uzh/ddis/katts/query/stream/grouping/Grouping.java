package ch.uzh.ddis.katts.query.stream.grouping;

import java.io.Serializable;

import backtype.storm.topology.BoltDeclarer;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This interface defines how a grouping has to be configured. A grouping
 * controls which variable binding is send to which parallelized node. 
 * 
 * @author Thomas Hunziker
 *
 */
public interface Grouping extends Serializable {
	
	/**
	 * This method checks if the grouping is well defined.
	 * @return
	 */
	public boolean validate();
	
	/**
	 * This method connects the consuming node with the stream by 
	 * setting the correct grouping.
	 * 
	 * @param bolt
	 * @param stream
	 */
	public void attachToBolt(BoltDeclarer bolt, StreamConsumer stream);

}
