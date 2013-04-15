/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.MinAggregatorConfiguration;

/**
 * This class is <b>not ThreadSafe</b>.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class MinAggregator extends Aggregator<Double> {

	private static final double DEFAULT_VALUE = Double.MAX_VALUE;

	/** The configuration object. */
	private MinAggregatorConfiguration configuration;

	/** The array of buckets. */
	private double[] buckets;

	/**
	 * We keep a copy of the current minimum in cache. The {@link #computeCurrentValue()} returns this value if the
	 * current value of {@link #cachedMinimum} is <code>null</code>. Everytime there is an update we reset this value to
	 * <code>null</code>.
	 */
	private Double cachedMinimum;

	/**
	 * Creates a new {@link MinAggregator} using the configuration object provided.
	 * 
	 * @param configuration
	 *            the configuration object.
	 * @param numberOfBuckets
	 *            the number of buckets this aggregator should use.
	 */
	public MinAggregator(MinAggregatorConfiguration configuration, int numberOfBuckets) {
		super(configuration.getAs());
		this.configuration = configuration;
		this.buckets = new double[numberOfBuckets];

		for (int i = 0; i < this.buckets.length; i++) {
			this.buckets[i] = MinAggregator.DEFAULT_VALUE;
		}
	}

	@Override
	public void extractAndIncorporateValue(SimpleVariableBindings bindings, int currentBucketIndex) {
		Object value = bindings.get(this.configuration.getOf());

		if (!(value instanceof Double)) {
			throw new IllegalArgumentException("The MinAggregator only supports values of type Double.");
		}
		double newValue = ((Double) value).doubleValue();
		double currentMinValue = this.buckets[currentBucketIndex];

		if (newValue < currentMinValue) {
			this.buckets[currentBucketIndex] = newValue;
			this.cachedMinimum = null; // re-compute when #computeCurrentValue() is called the next time
		}
	}

	@Override
	public void resetBucket(int bucketIndex) {
		this.buckets[bucketIndex] = MinAggregator.DEFAULT_VALUE;
		this.cachedMinimum = null; // re-compute when #computeCurrentValue() is called the next time
	}

	@Override
	public String getName() {
		return this.configuration.getAs();
	}

	@Override
	public Double computeCurrentValue() {
		if (this.cachedMinimum == null) {
			double currentMin = Double.MAX_VALUE;
			for (double tester : buckets) {
				if (tester < currentMin) {
					currentMin = tester;
				}
			}
			this.cachedMinimum = Double.valueOf(currentMin);
		}
		return this.cachedMinimum;
	}

}
