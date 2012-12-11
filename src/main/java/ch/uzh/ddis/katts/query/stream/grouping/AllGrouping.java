package ch.uzh.ddis.katts.query.stream.grouping;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import backtype.storm.topology.BoltDeclarer;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This {@link Grouping} sends all variable bindings to all linked nodes.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AllGrouping implements Grouping {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public void attachToBolt(BoltDeclarer bolt, StreamConsumer stream) {
		bolt.allGrouping(stream.getStream().getNode().getId(), stream.getStream().getId());
	}

}
