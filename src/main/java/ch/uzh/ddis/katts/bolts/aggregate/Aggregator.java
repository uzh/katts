package ch.uzh.ddis.katts.bolts.aggregate;

import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;

/**
 * This interface has to implemented by all aggregators that are to be used inside an instance of {@link Aggregator}.
 * 
 * @param <T>
 *            the type of object this aggregator aggregates over.
 */
public abstract class Aggregator<T> {

	/** The name of this aggregate in the query. */
	private String name;

	protected Aggregator(String name) {
		this.name = name;
	}

	/**
	 * Computes the current aggregator value over all the buckets.
	 * 
	 * @return the current value of the aggregator.
	 */
	public abstract T computeCurrentValue();

	/**
	 * Resets the value for a bucket. This method is called, when a bucket is reused. In a sum-aggregator, this method
	 * can for example substract the value of <b>bucket</b> from the current sum in this function, in order to maintain
	 * the correct over all aggregate value.
	 * 
	 * @param bucketIndex
	 *            the index of the bucket to be reset.
	 */
	public abstract void resetBucket(int bucketIndex);

	/**
	 * This method incorporates an additional <b>value</b> to the current aggregator. This method is typically called
	 * when a new value arrives.
	 * 
	 * @param bindings
	 *            the bindings object out of which the value can be extracted, which in turn can then be incorporated
	 *            into the provided bucket.
	 * @param currentBucketIndex
	 *            the index of the currently active bucket.
	 */
	public abstract void extractAndIncorporateValue(SimpleVariableBindings bindings, int currentBucketIndex);

	/**
	 * @return the name of the aggregate value.
	 */
	public String getName() {
		return this.name;
	}

}
