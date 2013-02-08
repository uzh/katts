/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.Date;
import java.util.LinkedList;

import javax.xml.datatype.Duration;

/**
 * A sum creator sums up integer values. Normally this sum is considered to be the sum "since the beginning of time"
 * meaning the sum does not lose values over time. If a time window is configured, the sum creator will remove items
 * from the current sum value depending on the time at which the current sum values is queried.
 * 
 * <b>Please Note:</b> Since this is an exact implementation of the sum operand, the amount of memory necessary to
 * compute a "windowed" sum can become quite large. Essentially, we have to store every single value that contributes to
 * the current sum in memory. Whenever we receive a new object (or the current state of the sum is queried), we need to
 * substract all elements that are no longer valid from the sum value.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class SumCreator {

	/** The current sum value */
	private double sum;

	/**
	 * In case of a windowed sum creator, we use this list to store all values that contribute to the current sum. Each
	 * array has exactly two elements, the first of which being the timestamp at which the value (the second element)
	 * was added to the sum.
	 */
	private final LinkedList<double[]> windowList;

	private final long windowSize;

	/**
	 * Creates a SumCreator for the specified windowSize. If the supplied window size is null, a non-windowed sum creator
	 * will be returned. 
	 * 
	 * @param windowSize the size of the window, the sum creator should have.
	 * @return the sum creator instance.
	 */
	public static SumCreator createSumCreator(Duration windowSize) {
		SumCreator result;
		
		if (windowSize == null) {
			result = new SumCreator();
		} else {
			result = new SumCreator(null);
		}
			
		return result;
	}
	
	
	/** Constructs a sum creator using no window size which will compute the sum "since the beginning of time". */
	private SumCreator() {
		this.windowSize = 0;
		this.windowList = null;
	}

	/** Constructs a sum creator using windowSize as the size of the window to compute the sum over */
	private SumCreator(Duration windowSize) {
		this.windowSize = windowSize.getTimeInMillis(new Date());
		this.windowList = new LinkedList<double[]>();
	}

	/**
	 * Adds <b>value</b> to the current sum taking into account the time value of <b>currentDate</b> If this is a
	 * windowed sum creator, all values that are older than <b>systemTime - windowSize</b> will be substracted from the
	 * sum before the new value is added.
	 * 
	 * @param systemTime
	 *            the current time of the system (most likely the endDate of the message that is currently being
	 *            processed).
	 * @param value
	 *            the value that is to be added to the current sum.
	 * @return the new sum value.
	 */
	public double add(long systemTime, double value) {
		if (this.windowSize > 0) {
			double[] element = this.windowList.peek();
			while ((element != null) && (element[0] < (systemTime - this.windowSize))) {
				this.sum -= element[1];
				this.windowList.remove();
				element = this.windowList.peek();
			}
		}
		
		//TODO: add elements to list!!

		this.sum += value;

		return this.sum;
	}

}
