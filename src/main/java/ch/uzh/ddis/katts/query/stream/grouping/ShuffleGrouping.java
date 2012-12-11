package ch.uzh.ddis.katts.query.stream.grouping;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import backtype.storm.topology.BoltDeclarer;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This {@link Grouping} sends the variable bindings randomly to the linked nodes.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ShuffleGrouping implements Grouping {

	private static final long serialVersionUID = 328341259453485756L;

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public void attachToBolt(BoltDeclarer bolt, StreamConsumer stream) {
		bolt.localOrShuffleGrouping(stream.getStream().getNode().getId(), stream.getStream().getId());
	}

}
