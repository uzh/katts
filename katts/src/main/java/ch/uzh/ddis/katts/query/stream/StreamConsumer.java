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
 * This class is the consuming part of a {@link Stream}. The {@link ConsumerNode} can
 * define by this class which stream should be consumed and how the stream should be grouped
 * (see {@link Grouping}).
 * Additionally some configuration on how the stream should be processed
 * can be defined.
 * 
 * @author Thomas Hunziker
 *
 */
public class StreamConsumer implements Serializable{
	
	@XmlTransient
	private static final long serialVersionUID = 1L;

	@XmlElementRefs({ 
		@XmlElementRef(type=AllGrouping.class), 
		@XmlElementRef(type=ShuffleGrouping.class), 
		@XmlElementRef(type=VariableGrouping.class), 
	})
	private Grouping grouping = new ShuffleGrouping();
	
	@XmlIDREF
	@XmlAttribute(name="streamId")
	private Stream stream;
	
	@XmlTransient
	private Node node;
	
	@XmlTransient
	private int maxBufferSize = 5;

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

	/**
	 * This method returns the maximal buffer size in processing
	 * this stream. The buffer size is usually used by synchronized
	 * nodes and joining nodes. The buffer size is relative to the
	 * number of variable bindings in it.
	 * 
	 * @return
	 */
	@XmlAttribute(name="maxBufferSize", required=true)
	public int getMaxBufferSize() {
		return maxBufferSize;
	}

	/**
	 * Sets the buffer size. See {@link #getMaxBufferSize()} for more information at
	 * the buffer size.
	 * 
	 * @param maxBufferSize
	 */
	public void setMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
	}
	
	// TODO: Implement the hashCode method

}
