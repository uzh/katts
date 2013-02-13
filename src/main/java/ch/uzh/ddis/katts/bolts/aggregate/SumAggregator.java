/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.SumAggregatorConfiguration;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SumAggregator extends Aggregator<Double> {

	private static final Double DEFAULT_VALUE = Double.valueOf(0.0D);

	/** The configuration object. */
	private SumAggregatorConfiguration configuration;

	/** The current sum of this aggregator. */
	private double currentSum;

	/** The array of buckets. */
	private Double[] buckets;

	/**
	 * Creates a new {@link SumAggregator} using the configuration object provided.
	 * 
	 * @param configuration
	 *            the configuration object.
	 * @param numberOfBuckets
	 *            the number of buckets this aggregator should use.
	 */
	public SumAggregator(SumAggregatorConfiguration configuration, int numberOfBuckets) {
		super(configuration.getAs());
		this.configuration = configuration;
		this.currentSum = 0.0D;
		this.buckets = new Double[numberOfBuckets];
	}

	@Override
	public void extractAndIncorporateValue(SimpleVariableBindings bindings, int currentBucketIndex) {
		Object value = bindings.get(this.configuration.getOf());
		
		if (!(value instanceof Double)) {
			throw new IllegalArgumentException("The SumAggregator only supports values of type Double.");
		}
		Double dValue = (Double) value;
		this.buckets[currentBucketIndex] += dValue;
		this.currentSum += dValue;
	}

	@Override
	public void resetBucket(int bucketIndex) {
		this.currentSum -= this.buckets[bucketIndex];
		this.buckets[bucketIndex] = SumAggregator.DEFAULT_VALUE;
	}

	@Override
	public String getName() {
		return this.configuration.getAs();
	}

	@Override
	public Double computeCurrentValue() {
		// since we kept a running sum, we don't need to compute anything here and can just return our current sum.
		return this.currentSum;
	}

}
