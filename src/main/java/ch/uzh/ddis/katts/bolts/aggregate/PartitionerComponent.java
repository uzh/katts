package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.List;

/**
 * A partitioner component builds an aggregate on a {@link Variable} for a partition with a slide size and a window
 * size. This interface defines the set of methods required to be used as a component of the {@link PartitionerBolt}.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface PartitionerComponent {

	/**
	 * This method is invoked, whenever the bucket storage must be reset or initialized. The method must return any
	 * object, that should be stored for a bucket.
	 */
	public Object resetBucket();

	/**
	 * This method is called whenever a bucket must be updated.
	 * 
	 * @param storage
	 *            The object in which the bucket state must be stored
	 * @param number
	 *            The number of the current event on which the aggregation is done.
	 * @return The method must return the changed storage object.
	 */
	public Object updateBucket(Object storage, double number);

	/**
	 * This method calculates the aggregate from the buckets list of this specific component.
	 * 
	 * @param componentBuckets
	 *            The list of storage objects for this component.
	 * @return The aggregate from this list.
	 */
	public Double calculateAggregate(List<Object> componentBuckets);

	/**
	 * This method returns the name of the the component. In case of a min component this should return "min". This
	 * value is used to access the bucket storage and it is used to emit to the corresponding variable.
	 * 
	 * @return Returns the name of the component.
	 */
	public String getName();

}
