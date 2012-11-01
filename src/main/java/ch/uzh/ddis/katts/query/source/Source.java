package ch.uzh.ddis.katts.query.source;

import ch.uzh.ddis.katts.query.Node;
import ch.uzh.ddis.katts.query.SpoutNode;

/**
 * A Source Node represents a node which does only produce data and do
 * not consumes any.
 * 
 * @author Thomas Hunziker
 *
 */
public interface Source extends SpoutNode,Node {
}
