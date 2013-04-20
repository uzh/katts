package ch.uzh.ddis.katts.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * This queue helps a to (totally) order a series of objects and provides tolerance concerning the the order in which
 * these events are added to this queue. A user places new elements into the queue by calling {@link #offer(Object)} and
 * gets a collection of elements to process in return. This collection is guaranteed to be in ascending order. To
 * prevent the situation when an element that is added later during runtime to be out-of-order, this class allows the
 * user to specify a minimal delay, during which the queue keeps the elements in its cache. Note, that this doesn't mean
 * that processing is guaranteed to take place immediately after this minimal delay. If the queue receives elements that
 * have a lower semantic ordering (in terms of the specified comparator), it waits for these events to "time out" first.
 * Therefore, it could theoretically be that an element gets put into this queue at the very beginning of the program
 * run but stays in there, because there are always new elements added to the queue that are lower in terms of their
 * semantic ordering, but younger in terms of when they were added to the queue.
 * 
 * This class is <b>not thread safe</b>, so access to it has to be synchronized.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class ElasticPriorityQueue<E> implements Serializable {

	private PriorityQueue<ElemWrap> priorityQueue;

	/**
	 * This is the delay (milliseconds) during which elements are kept in the cache before they are returned.
	 */
	private long delay;

	/**
	 * Creates a new elastic priority queue with the given delay.
	 * 
	 * @param delay
	 *            the delay in milliseconds.
	 */
	public ElasticPriorityQueue(long delay, Comparator<E> comparator) {
		this.delay = delay;
		this.priorityQueue = new PriorityQueue<ElemWrap>(1000, new ElemWrapComparator(comparator));
	}

	public Collection<E> offer(E e) {
		ArrayList<E> result = new ArrayList<E>();
		
		if (e == null) {
			throw new NullPointerException("WTF??");
		}
		// add the element to the queue
		this.priorityQueue.offer(new ElemWrap(e));

		// add all elements to the resulting list whose timestamp is smaller than [now - delay]
		while (true) {
			ElemWrap preview = this.priorityQueue.peek();
			if (preview == null) {
				break;
			}
			if (preview.timestamp < System.currentTimeMillis() - this.delay) {
				result.add(this.priorityQueue.poll().element);
			} else {
				break; // no more elements should be returned
			}
		}

		// TODO: compute if the delay should be changed

		return result;
	}

	/**
	 * Returns and removes all elements from this queue.
	 * 
	 * @return all remaining elements in the queue;
	 */
	public Collection<E> drainElements() {
		ArrayList<E> result = new ArrayList<E>();

		while (!this.priorityQueue.isEmpty()) {
			ElemWrap wrapper = this.priorityQueue.poll();
			if (wrapper != null) { // this should be possible
				result.add(wrapper.element);
			}
		}

		return result;
	}

	/**
	 * @return true, if there are no elements left in this queue.
	 */
	public boolean isEmpty() {
		return this.priorityQueue.peek() != null;
	}

	/**
	 * This class wraps the elements that have to be stored by the ElasticPriorityQueue. It keeps the reference to the
	 * element and attaches the current system timestamp to is. This time stamp is used to determine the time for which
	 * a element has to be kept in cache.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 * 
	 */
	private final class ElemWrap implements Serializable {
		private final E element;
		private final long timestamp;

		public ElemWrap(E element) {
			this.element = element;
			this.timestamp = System.currentTimeMillis();
		}
	}

	/**
	 * This comparator compares elements of type ElemWrap using a user-supplied comaprator to compare the inner Elements
	 * of type E.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 */
	private final class ElemWrapComparator implements Comparator<ElemWrap>, Serializable {

		/** The comparator to compare the elemements wrapped by ElemWrap. */
		private final Comparator<E> comparator;

		/**
		 * Creates a comparator for ElemWrap objects using the provided comparator to compare the wrapped objects that
		 * are inside the ElemWrap objects.
		 * 
		 * @param elementComparator
		 *            a comparator that compares the objects wrapped inside the ElemWrap objects.
		 */
		public ElemWrapComparator(Comparator<E> comparator) {
			this.comparator = comparator;
		}

		@Override
		public int compare(ElemWrap o1, ElemWrap o2) {
			if (o1 == null && o2 == null) return 0;
			if (o1 == null && o2 != null) return -1;
			if (o1 != null && o2 == null) return 1;
			return this.comparator.compare(o1.element, o2.element);
		}

	}

}
