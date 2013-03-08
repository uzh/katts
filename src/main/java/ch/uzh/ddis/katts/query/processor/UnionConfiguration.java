package ch.uzh.ddis.katts.query.processor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.UnionBolt;

/**
 * The XML configuration object for the "union" node of KATTS. This node sends the messages from all incoming streams to
 * all outgoing streams. If any of the configured fields on the outgoig streams is empty, a null value will be
 * transmitted for this field.
 * 
 * See {@link UnionBolt} for the concrete bolt implementation.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * @see UnionBolt
 */
@XmlRootElement(name = "union")
@XmlAccessorType(XmlAccessType.FIELD)
public class UnionConfiguration extends AbstractProcessor {

	@Override
	public Bolt createBoltInstance() {
		return new UnionBolt(this);
	}
	

}
