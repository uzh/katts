package ch.uzh.ddis.katts.query.output;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.query.AbstractNode;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * The AbstractOutput does provide an convinient interface for
 * building an output bolt. An output bolt is a bolt which does
 * not send the data to another bolt. It may send it to a message 
 * queue or writing to the file.
 * 
 * @author Thomas Hunziker
 *
 */
public abstract class AbstractOutput extends AbstractNode implements Output {
	
	private static final long serialVersionUID = 1L;
	
	@XmlTransient
	private List<StreamConsumer> consumers = new ArrayList<StreamConsumer>();
	
	@Override
	@XmlElementWrapper(name="consumes")
	@XmlElement(name="stream")
	public List<StreamConsumer> getConsumers() {
		for (StreamConsumer consumer : consumers) {
			consumer.setNode(this);
		}
		
		return consumers;
	}

	public void setConsumers(List<StreamConsumer> consumers) {
		this.consumers = consumers;
	}
	
	public void appendConsumer(StreamConsumer consumer) {
		this.getConsumers().add(consumer);
	}

	@Override
	@XmlTransient
	public int getParallelism() {
		// We assume that a regular output is not parallelizable  
		return 1;
	}


}
