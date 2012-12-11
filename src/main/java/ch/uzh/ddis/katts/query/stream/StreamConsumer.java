package ch.uzh.ddis.katts.query.stream;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.query.ConsumerNode;
import ch.uzh.ddis.katts.query.Node;
import ch.uzh.ddis.katts.query.stream.grouping.AllGrouping;
import ch.uzh.ddis.katts.query.stream.grouping.Grouping;
import ch.uzh.ddis.katts.query.stream.grouping.ShuffleGrouping;
import ch.uzh.ddis.katts.query.stream.grouping.VariableGrouping;

/**
 * This class is the consuming part of a {@link Stream}. The {@link ConsumerNode} can define by this class which stream
 * should be consumed and how the stream should be grouped (see {@link Grouping}). Additionally some configuration on
 * how the stream should be processed can be defined.
 * 
 * @author Thomas Hunziker
 * 
 */
public class StreamConsumer implements Serializable {

	@XmlTransient
	private static final long serialVersionUID = 1L;

	@XmlElementRefs({ @XmlElementRef(type = AllGrouping.class), @XmlElementRef(type = ShuffleGrouping.class),
			@XmlElementRef(type = VariableGrouping.class), })
	private Grouping grouping = new ShuffleGrouping();

	@XmlIDREF
	@XmlAttribute(name = "streamId")
	private Stream stream;

	@XmlTransient
	private Node node;

	@XmlTransient
	private long bufferTimeout = -1;

	@XmlTransient
	public Stream getStream() {
		return stream;
	}

	public void setStream(Stream stream) {
		this.stream = stream;
	}

	@XmlTransient
	public Grouping getGrouping() {
		return grouping;
	}

	public void setGrouping(Grouping grouping) {
		this.grouping = grouping;
	}

	@XmlTransient
	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

//	/**
//	 * This method returns the buffer timeout. (See {@link StreamConsumer#getBufferTimeout()}) But this method returns
//	 * instead of -2 the default value. This method should be used for retrieving the timeout value to use in the bolt
//	 * code.
//	 * 
//	 * @return The number of milliseconds an event is queued in the buffer. -1 indicates an infinite timeout.
//	 */
//	@XmlTransient
////	public long getRealBufferTimout() {
////
////		long timeout = getBufferTimeout();
////
////		if (timeout == -2) {
////			return this.getNode().getQuery().getDefaultBufferTimeout();
////		} else {
////			return timeout;
////		}
////	}
//
//	/**
//	 * The buffer timeout defines the number of milliseconds an event is stored in the buffer. The timeout is defined
//	 * relative to the event time and not to the system time.
//	 * 
//	 * @return The number of milliseconds an event is queued in the buffer. -1 indicates an infinite timeout and -2
//	 *         indicates the default timeout.
//	 */
//	@XmlAttribute(name = "bufferTimeout")
//	public long getBufferTimeout() {
//		return bufferTimeout;
//	}

	public void setBufferTimeout(long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	// TODO: Implement the hashCode method

}
