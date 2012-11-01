package ch.uzh.ddis.katts.query;

import backtype.storm.topology.IRichSpout;

/**
 * A spout node is a Node configuration for a Spout. A Spout is
 * a program, that only emits data. 
 * 
 * @author Thomas Hunziker
 *
 */
public interface SpoutNode extends Node{

	/**
	 * This method returns the effective Spout implementation.
	 * 
	 * @return
	 */
	public IRichSpout getSpout();
	
}
