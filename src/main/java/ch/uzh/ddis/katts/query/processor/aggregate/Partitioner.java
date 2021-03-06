package ch.uzh.ddis.katts.query.processor.aggregate;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.aggregate.PartitionerBolt;
import ch.uzh.ddis.katts.bolts.aggregate.PartitionerComponent;
import ch.uzh.ddis.katts.bolts.aggregate.PartitionerConfiguration;
import ch.uzh.ddis.katts.query.processor.AbstractSynchronizedProcessor;
import ch.uzh.ddis.katts.query.processor.aggregate.component.AvgPartitioner;
import ch.uzh.ddis.katts.query.processor.aggregate.component.MaxPartitioner;
import ch.uzh.ddis.katts.query.processor.aggregate.component.MinPartitioner;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * The partitioner node creates a sliding window over the stream of variable bindings and applies one or more
 * aggregation operators on the contents of the window. The aggregate values are computed over a given subset of the
 * variable bindings. These subsets are defined using the "partitionOn" field (equivalent to the group by clause in
 * SQL). The aggreagte values are computed on the field specified in the "aggregateOn" attribute.
 * 
 * Each partitioner creates a sliding window using a number buckets defined by the window size and the slide size (the
 * step by which the window advances). Values of a bucket is emitted, when the current bucket time has expired and a new
 * bucket has started to "fill up". This means that values will only be emitted when the aggregate value has changed.
 * 
 * The window size and step size are defined using the XML schema notation for durations documented in the
 * {@link DatatypeFactory#newDuration(String)} method.
 * 
 * Currently the maximum (max) and the minimum (min) aggregation operators are supported.
 * 
 * @see MaxPartitioner
 * @see MinPartitioner
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Partitioner extends AbstractSynchronizedProcessor implements PartitionerConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlIDREF
	@XmlAttribute(required = true)
	private Variable aggregateOn;

	@XmlIDREF
	@XmlAttribute(required = true)
	private Variable partitionOn;

	@XmlAttribute(required = true)
	private Duration windowSize;

	@XmlAttribute(required = true)
	private Duration slideSize;

	@XmlElementRefs({ @XmlElementRef(type = MaxPartitioner.class), 
					  @XmlElementRef(type = MinPartitioner.class),
					  @XmlElementRef(type = AvgPartitioner.class) })
	@XmlElementWrapper(name = "components")
	private List<PartitionerComponent> components = new ArrayList<PartitionerComponent>();

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public Variable getAggregateOn() {
		return aggregateOn;
	}

	public void setAggregateOn(Variable aggregateOn) {
		this.aggregateOn = aggregateOn;
	}

	@Override
	public Duration getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(Duration windowSize) {
		this.windowSize = windowSize;
	}

	@Override
	public Duration getSlideSize() {
		return slideSize;
	}

	public void setSlideSize(Duration slideSize) {
		this.slideSize = slideSize;
	}

	@Override
	public Bolt createBoltInstance() {
		return new PartitionerBolt(this);
	}

	@Override
	public Variable getPartitionOn() {
		return partitionOn;
	}

	public void setPartitionOn(Variable partitionOn) {
		this.partitionOn = partitionOn;
	}

	@Override
	public List<PartitionerComponent> getComponents() {
		return components;
	}

	public void setComponents(List<PartitionerComponent> components) {
		this.components = components;
	}

	public void appendComponent(PartitionerComponent component) {
		this.getComponents().add(component);
	}
}
