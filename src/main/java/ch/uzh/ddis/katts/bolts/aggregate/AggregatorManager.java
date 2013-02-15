package ch.uzh.ddis.katts.bolts.aggregate;

import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregatorConfiguration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

/**
 * The AggregatorManager maintains a list of buckets and a list of aggregator instances that compute their aggregate
 * values of these buckets. The list of buckets is maintained as a linked list containing as many
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class AggregatorManager {

	public static interface Callback {
		/**
		 * This method will be called whenever the update interval is reached, and the current values of the aggregators
		 * should be propagated.
		 * 
		 * @param aggregateValues
		 *            A table containing all the aggregate values. The row key of the table is the list of the values of
		 *            the group-by key over which we group, the column keys are the names of the aggregate values and
		 *            the the values are the actual aggregate values that have been used in the last callback.
		 * @param startDate
		 *            the start date over which the values have been aggregated.
		 * @param endDate
		 *            the end date over which the values have been aggregated.
		 */
		public void callback(Table<ImmutableList<Object>, String, Object> aggregateValues, Date startDate, Date endDate);
	}

	/** The index of the current bucket to use. */
	private int currentBucketIndex;

	/** The duration during which a bucket is used, before switching to the next bucket. */
	private final long bucketDuration;

	/**
	 * These are the times for which each bucket contains the values. All elements that have a time value (typically the
	 * endDate of the variable binding) that is smaller than the bucketTime + bucketDuration belong into the same
	 * bucket.
	 */
	private long[] bucketTimes;

	/** The number of buckets is computed by dividing the window size by the bucket duration. */
	private final int numberOfBuckets;

	/** The interval in which updates will be propagated milliseconds. */
	private final long updateInterval;

	/** The size of the window in milliseconds. */
	private final long windowSize;

	/** This is the time at which we have last sent an update using the callback method. */
	private long lastUpdateSent;

	/** The method we call, whenever an update is needed. */
	private Callback callback;

	/** This map contains an array of aggregators for each key that we group over. */
	private final Map<ImmutableList<Object>, Aggregator<?>[]> aggregators;

	/**
	 * This table holds the values for each aggregate that has last been sent using the callback. This is needed so we
	 * can suppress sending of updates, if the values have not changed.
	 * 
	 * The row key of the table is the list of fields over which we group, the column keys are the names of the
	 * aggregate values and the the values are the actual aggregate values that have been used in the last callback.
	 */
	private final Table<ImmutableList<Object>, String, Object> lastSentAggregates;

	/**
	 * if this value is set to true, the callback will only contain values that have changed, since the last callback.
	 */
	private boolean onlyReportIfValuesChanged;

	/**
	 * Creates a new {@link AggregatorManager} instance.
	 * 
	 * @param windowSize
	 *            the size of the window over which the aggregates should be computed.
	 * @param updateInterval
	 *            the interval at which updates should be propagated using the callbackMethod.
	 * @param callback
	 *            the object containing the callback method to call, whenever the updateInterval has been reached.
	 * @param onlyReportIfValuesChanged
	 *            if this value is set to true, the callback will only contain values that have changed, since the last
	 *            callback.
	 * @param aggregatorConfigs
	 *            a list containing configuration objects for all aggregators that are managed by this manager.
	 */
	public AggregatorManager(Duration windowSize, Duration updateInterval, Callback callback,
			boolean onlyCallbackIfValuesChanged, final AggregatorConfiguration<?>... aggregatorConfigs) {
		Date currentDate = new Date(); // we use the current date to convert durations to millisecond values

		this.callback = callback;
		this.onlyReportIfValuesChanged = onlyCallbackIfValuesChanged;
		this.lastSentAggregates = HashBasedTable.create();

		/*
		 * Compute the duration, during which a bucket is going to stay active. This value is the greates common divisor
		 * (GCD) of the windowSize and the updateInterval.
		 */
		this.windowSize = windowSize.getTimeInMillis(currentDate);
		this.updateInterval = updateInterval.getTimeInMillis(currentDate);
		this.bucketDuration = BigInteger.valueOf(this.windowSize).gcd(BigInteger.valueOf(this.updateInterval))
				.longValue();
		this.numberOfBuckets = Long.valueOf(this.windowSize / this.bucketDuration).intValue();

		this.bucketTimes = new long[this.numberOfBuckets];
		this.bucketTimes[0] = -1; // set a marker to initialize the bucket time upon first usage

		// the aggregators map has creates a new list of aggregators for each group-by key
		this.aggregators = new HashMap<ImmutableList<Object>, Aggregator<?>[]>() {
			@SuppressWarnings("unchecked")
			public Aggregator<?>[] get(Object key) {
				Aggregator<?>[] result = super.get(key);

				if (result == null) {
					result = new Aggregator[aggregatorConfigs.length];
					for (int i = 0; i < aggregatorConfigs.length; i++) {
						result[i] = aggregatorConfigs[i].createInstance(AggregatorManager.this.numberOfBuckets);
					}
					put((ImmutableList<Object>) key, result);
				}

				return result;
			}
		};

	}

	/**
	 * Adds a new value to the aggregates maintained by this manager.
	 * 
	 * @param groupByKey
	 *            the grouping key
	 * @param bindings
	 *            the variable bindings object that has been received.
	 */
	public void incorporateValue(ImmutableList<? extends Object> groupByKey, SimpleVariableBindings bindings) {


		// check if we have to switch buckets or propagate updates using the callback
		advanceInTime(bindings.getEndDate());
		
		// add value to aggregator
		for (Aggregator<?> aggregator : this.aggregators.get(groupByKey)) {
			aggregator.extractAndIncorporateValue(bindings, this.currentBucketIndex);
		}
	}

	/**
	 * This method moves the current time of the manager and informs about updates of the aggregate values if necessary.
	 * This method will typically be called by an external timer such as the heartbeat of the system or similar.
	 * 
	 * @param currentTime
	 *            the time up to which the state of the datastructures should be advanced.
	 */
	public void advanceInTime(long currentTime) {
	
		/*
		 * When this method is called for the first time, the first bucket will have no time set. In that case
		 * we initialize the value to be the current processing time as given by currenttime.
		 */
		if (this.bucketTimes[this.currentBucketIndex] < 0) {
			this.bucketTimes[this.currentBucketIndex] = currentTime;
		}
		
		while (currentTime >= this.bucketTimes[this.currentBucketIndex] + this.bucketDuration) {
			long newBucketTime;

			// check if we need to send an update
			if ((this.lastUpdateSent + updateInterval) < (this.bucketTimes[this.currentBucketIndex] + this.bucketDuration)) {
				long startDate;
				long endDate;
				Table<ImmutableList<Object>, String, Object> aggregates = HashBasedTable.create();

				for (ImmutableList<Object> groupByKey : this.aggregators.keySet()) {
					for (Aggregator<?> aggregator : aggregators.get(groupByKey)) {
						Object lastSentValue = this.lastSentAggregates.get(groupByKey, aggregator.getName());
						Object currentValue = aggregator.computeCurrentValue();

						if (!onlyReportIfValuesChanged || !currentValue.equals(lastSentValue)) {
							this.lastSentAggregates.put(groupByKey, aggregator.getName(), currentValue);
							aggregates.put(groupByKey, aggregator.getName(), currentValue);
						}
					}
				}

				startDate = this.bucketTimes[computeBucketIndexBefore(this.currentBucketIndex)];
				endDate = startDate + AggregatorManager.this.windowSize;
				this.callback.callback(aggregates, new Date(startDate), new Date(endDate));
			}

			// compute the time for the next bucket
			newBucketTime = this.bucketTimes[this.currentBucketIndex] + this.bucketDuration;

			// move the pointer to the next bucket
			this.currentBucketIndex = computeBucketIndexAfter(this.currentBucketIndex);

			// reset the bucket at the (now new) current index
			this.bucketTimes[this.currentBucketIndex] = newBucketTime;
			for (ImmutableList<Object> groupByKey : this.aggregators.keySet()) {
				for (Aggregator<?> aggregator : aggregators.get(groupByKey)) {
					aggregator.resetBucket(this.currentBucketIndex);
				}
			}
		}
	}

	/**
	 * Computes the bucket index that follows after <code>index</code>.
	 * 
	 * @param index
	 *            the index to base the calculation on.
	 * @return the index of the bucket that follows the one at position <code>index</code>.
	 */
	private int computeBucketIndexAfter(int index) {
		index++;
		if (index >= this.numberOfBuckets) {
			index = 0;
		}
		return index;
	}

	/**
	 * Computes the bucket index that follows after <code>index</code>.
	 * 
	 * @param index
	 *            the index to base the calculation on.
	 * @return the index of the bucket that follows the one at position <code>index</code>.
	 */
	private int computeBucketIndexBefore(int index) {
		index--;
		if (index < 0) {
			index = this.numberOfBuckets - 1;
		}
		return index;
	}
}
