package ch.uzh.ddis.katts.query.processor.aggregate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import ch.uzh.ddis.katts.bolts.aggregate.Aggregator;

/**
 * All aggregrator configuration objects (i.e. the configuration objects for aggregator components of the
 * AggregateConfiguration) have to implement this interface.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class AggregatorConfiguration<T extends Aggregator<?>> {

	/** Creates an instance of the aggregator this configuration object is for. */
	public abstract T createInstance(int numberOfBuckets);
	
	/**
	 * The value of the sum can be referenced in the variable list of the outgoing streams using this name.
	 * 
	 * Example: The following configuration:
	 * 
	 * <pre>
	 * &lt;computeSum of="PRC" as="teh_sum" /&gt;
	 * </pre>
	 * 
	 * would allow you to use the variable "sum" in the variable configuration as follows:
	 * 
	 * <pre>
	 * &lt;variable type="xs:long" name="sumOfPrc" referencesTo="teh_sum" /&gt;
	 * </pre>
	 */
	@XmlAttribute(required = true)
	private String as;
	
	/**
	 * {@link SumAggregatorConfiguration#as}
	 * 
	 * @return the as
	 */
	public String getAs() {
		return as;
	}

	/**
	 * {@link SumAggregatorConfiguration#as}
	 * 
	 * @param as
	 *            the as to set
	 */
	public void setAs(String as) {
		this.as = as;
	}
	
}
