package ch.uzh.ddis.katts.query.processor;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

import ch.uzh.ddis.katts.query.AbstractNode;
import ch.uzh.ddis.katts.query.stream.Producers;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This class implements an abstract processor. It provides convenient configuration methods for consuming, processing
 * and emitting data.
 * 
 * These are primarily the handling of the stream configurations.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class AbstractProcessor extends AbstractNode implements Processor {

	private static final long serialVersionUID = 1L;

	/**
	 * The number of task instances that should be started for this bolt. If you set this to 0, so task will be started!
	 */
	@XmlAttribute
	private int parallelism = 1;

	@XmlElementWrapper(name = "produces")
	@XmlElement(name = "stream")
	private List<Stream> producers = new Producers(this);

	@XmlElementWrapper(name = "consumes")
	@XmlElement(name = "stream")
	private List<StreamConsumer> consumers = new ArrayList<StreamConsumer>();

	@Override
	public List<StreamConsumer> getConsumers() {
		for (StreamConsumer consumer : consumers) {
			consumer.setNode(this);
		}

		return consumers;
	}

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

	@Override
	public int getParallelism() {
		return this.parallelism;
	}

	public void setParallelism(int paralleism) {
		this.parallelism = paralleism;
	}

}