package ch.uzh.ddis.katts.query.source;

import backtype.storm.topology.IRichBolt;
import ch.uzh.ddis.katts.query.Node;

/**
 * A Source Node represents a node which does only produce data and do not consumes any.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface Source extends Node {

	public IRichBolt getBolt();

}
