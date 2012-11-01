package ch.uzh.ddis.katts.bolts.aggregate.component;

import java.util.List;

import ch.uzh.ddis.katts.bolts.aggregate.PartitionerComponent;

/**
 * This class implements the minimal calculation for a partition.
 * 
 * @author Thomas Hunziker
 *
 */
public class MinPartitionerComponent implements PartitionerComponent{

	@Override
	public Object resetBucket() {
		return null;
	}

	@Override
	public Object updateBucket(Object storage, double number) {
		Double internalStorage;
		if (storage == null) {
			internalStorage = number;
		}
		else {
			internalStorage = (Double)storage;
			if (internalStorage > number) {
				internalStorage = number;
			}
		}
		return internalStorage;
	}

	@Override
	public double calculateAggregate(List<Object> componentBuckets) {
		Double min = null;
		for (Object bucketValue : componentBuckets) {
			if (bucketValue == null) {
				continue;
			}
			Double internalBucketValue = (Double) bucketValue;
			if (min == null) {
				min = internalBucketValue;
			}
			else if(min > internalBucketValue) {
				min = internalBucketValue;
			}
		}
		
		return min;
	}

	@Override
	public String getName() {
		return "min";
	}

}
