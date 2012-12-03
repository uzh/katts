package ch.uzh.ddis.katts.bolts.aggregate.component;

/**
 * This class is used for the storage items for the average partition component.
 * 
 * It is a tuple storage for the count and the sum of a bucket.
 * 
 * @see AvgPartitionerComponent
 * 
 * @author Thomas Hunziker
 * 
 */
public class AvgStorageItem {

	private long count = 0;

	private double sum = 0;

	/**
	 * This method updates the inner state with an additional number.
	 * 
	 * @param number
	 */
	public synchronized void addItem(double number) {
		sum += number;
		count++;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

}
