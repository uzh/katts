package ch.uzh.ddis.katts.query.source;

import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.query.AbstractNode;

/**
 * This class is an abstract implementation of an source node.
 * 
 * @see Source
 * 
 * @author Thomas Hunziker
 *
 */
public abstract class AbstractSource extends AbstractNode implements Source{

	private static final long serialVersionUID = 1L;

	@Override
	@XmlTransient
	public int getParallelism() {
		// We assume that a regular source is not parallelizable  
		return 1;
	}


}
