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

	private static final double DEFAULT_VALUE = 0.0D;

	/** The configuration object. */
	private SumAggregatorConfiguration configuration;

	/** The current sum of this aggregator. */
	private double currentSum;

	/** The array of buckets. */
	private double[] buckets;

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
		this.buckets = new double[numberOfBuckets];

		for (int i = 0; i < this.buckets.length; i++) {
			this.buckets[i] = SumAggregator.DEFAULT_VALUE;
		}
	}

	@Override
	public void extractAndIncorporateValue(SimpleVariableBindings bindings, int currentBucketIndex) {
		Object value = bindings.get(this.configuration.getOf());

		if (!(value instanceof Double)) {
			throw new IllegalArgumentException("The SumAggregator only supports values of type Double.");
		}
		double dValue = ((Double) value).doubleValue();
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
