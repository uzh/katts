/**
 * 
 */
package ch.uzh.ddis.katts.query.processor.aggregate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.aggregate.SumAggregator;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
@XmlRootElement(name = "sum")
@XmlAccessorType(XmlAccessType.FIELD)
public class SumAggregatorConfiguration extends AggregatorConfiguration<SumAggregator> {

	/** This is the field the sum will be computed over. */
	@XmlAttribute(required = true)
	private String of;

	/**
	 * {@link SumAggregatorConfiguration#of}
	 * 
	 * @return the of
	 */
	public String getOf() {
		return of;
	}

	/**
	 * {@link SumAggregatorConfiguration#of}
	 * 
	 * @param of
	 *            the of to set
	 */
	public void setOf(String of) {
		this.of = of;
	}

	@Override
	public SumAggregator createInstance(int numberOfBuckets) {
		return new SumAggregator(this, numberOfBuckets);
	}

}
