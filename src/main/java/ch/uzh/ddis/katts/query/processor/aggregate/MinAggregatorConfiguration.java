/**
 * 
 */
package ch.uzh.ddis.katts.query.processor.aggregate;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.aggregate.MinAggregator;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
@XmlRootElement(name = "min")
@XmlAccessorType(XmlAccessType.FIELD)
public class MinAggregatorConfiguration extends AggregatorConfiguration<MinAggregator> {

	/** This is the field the sum will be computed over. */
	@XmlAttribute(required = true)
	private String of;

	/**
	 * {@link MinAggregatorConfiguration#of}
	 * 
	 * @return the of
	 */
	public String getOf() {
		return of;
	}

	/**
	 * {@link MinAggregatorConfiguration#of}
	 * 
	 * @param of
	 *            the of to set
	 */
	public void setOf(String of) {
		this.of = of;
	}

	@Override
	public MinAggregator createInstance(int numberOfBuckets) {
		return new MinAggregator(this, numberOfBuckets);
	}

}
