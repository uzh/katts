package ch.uzh.ddis.katts.query.processor.aggregate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.aggregate.SumBolt;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

/**
 * The XML configuration object for the "sum" node of KATTS. This node computes the sum of a field over a specified time
 * window. It supports grouping over one or multiple fields and supports a configurable output interval.
 * 
 * See {@link SumBolt} for the concrete bolt implementation.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * @see SumBolt
 */
@XmlRootElement(name = "sum")
@XmlAccessorType(XmlAccessType.FIELD)
public class SumConfiguration extends AbstractProcessor {

	/** This is the field the sum will be computed over. */
	@XmlAttribute(required = true)
	private String field;

	/** The name of the variable that should contain the sum value. */
	@XmlAttribute(required = true)
	private String as;
	
	/**
	 * This is analoguous to the "group by" statement of an SQL query. A comma separated list of variable names, over
	 * which the sum should be computed. If this field is missing, the sum will be computed over all incoming messages.
	 */
	@XmlAttribute(required = false)
	private String groupBy;

	/**
	 * The size of the window, over which the sum should be computed. If this attribute is missing, the window is
	 * considered to be infinite and the sum will be computed "from the beginning of time".
	 * 
	 * The value of this window has to be specified as defined in the W3C XML Schema 1.0 specification for time spans.
	 */
	@XmlAttribute(required = false)
	private Duration windowSize;

//	/**
//	 * The interval in which the current state of the node should be communicated to the following nodes in the
//	 * topology. If this attribute is missing, the interval is considered to be "instant", which means that the node
//	 * emits the sums whenever the value changes. This could result in long periods without updates for certain sums.
//	 * 
//	 * The value of this interval has to be specified as defined in the W3C XML Schema 1.0 specification for time spans.
//	 */
//	@XmlAttribute(required = false)
//	private Duration outputInterval;

	@Override
	public Bolt createBoltInstance() {
		return new SumBolt(this);
	}

	/**
	 * @return the field
	 */
	public String getField() {
		return field;
	}

	/**
	 * @param field the field to set
	 */
	public void setField(String field) {
		this.field = field;
	}
	
	/**
	 * @return the as
	 */
	public String getAs() {
		return as;
	}

	/**
	 * @param as the as to set
	 */
	public void setAs(String as) {
		this.as = as;
	}

	/**
	 * @return the groupBy
	 */
	public String getGroupBy() {
		return groupBy;
	}

	/**
	 * @param groupBy the groupBy to set
	 */
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	/**
	 * @return the windowSize
	 */
	public Duration getWindowSize() {
		return windowSize;
	}

	/**
	 * @param windowSize the windowSize to set
	 */
	public void setWindowSize(Duration windowSize) {
		this.windowSize = windowSize;
	}

//	/**
//	 * @return the outputInterval
//	 */
//	public Duration getOutputInterval() {
//		return outputInterval;
//	}
//
//	/**
//	 * @param outputInterval the outputInterval to set
//	 */
//	public void setOutputInterval(Duration outputInterval) {
//		this.outputInterval = outputInterval;
//	}

}
