package ch.uzh.ddis.katts.query.processor;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.query.AbstractNode;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This class implements an abstract processor. It provides convenient configuration
 * methods for consuming, processing and emitting data.
 * 
 * These are primarily the handling of the stream configurations.
 * 
 * @author Thomas Hunziker
 *
 */
public abstract class AbstractProcessor extends AbstractNode implements Processor{

	private static final long serialVersionUID = 1L;

	@XmlTransient
	private List<StreamConsumer> consumers = new ArrayList<StreamConsumer>();
	
	@XmlTransient
	private List<Stream> producers = new ArrayList<Stream>();
	
	@XmlElementWrapper(name="consumes")
	@XmlElement(name="stream")
	@Override
	public List<StreamConsumer> getConsumers() {
		for (StreamConsumer consumer : consumers) {
			consumer.setNode(this);
		}
		
		return consumers;
	}
	
	@XmlElementWrapper(name="produces")
	@XmlElement(name="stream")
	@Override
	public List<Stream> getProducers() {
		for (Stream producer : producers) {
			producer.setNode(this);
		}
		
		return producers;
	}

	public void setConsumers(List<StreamConsumer> consumers) {
		this.consumers = consumers;
	}

	public void setProducers(List<Stream> producers) {
		this.producers = producers;
	}
	
	public void appendConsumer(StreamConsumer consumer) {
		this.getConsumers().add(consumer);
	}
	
	public void appendProducer(Stream producer) {
		this.getProducers().add(producer);
	}
	
	@Override
	@XmlTransient
	public int getParallelism() {
		// We assume that a regular processor is fully parallelizable  
		return 0;
	}
	
}
