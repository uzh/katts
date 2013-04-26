package ch.uzh.ddis.katts.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Queue implementations that need to sort their input can extend from this class. It provides basic facilities such as
 * a timer to flush the queue in a configurable interval, a framework for callback functions, and an object wrapper
 * along with a comparator.
 * 
 * This class is <b>not thread safe</b>, so access to it has to be synchronized.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public abstract class AbstractFlushQueue<E> implements Serializable {

	/**
	 * This is the timeout (milliseconds) after which the
	 */
	private long flushTimeout;

	/** This id is used to name the timer thread. This is helpful for debugging. */
	private String timerId;

	/**
	 * We use this timer to make sure, someone is "clearing the pipes" even if there are no more elements being added to
	 * the queue.
	 */
	private Timer flushTimer;

	/** This timer task will be called whenever the flush timeout is reached. */
	private TimerTask flushTask;

	/**
	 * Creates a new elastic priority queue with the given delay.
	 * 
	 * @param flushTimeout
	 *            the timeout (milliseconds) after which the queue flushed. Every call to
	 * @param timerId
	 *            a textual id for the timer task. This is useful for debugging.
	 */
	public AbstractFlushQueue(long flushTimeout, String timerId) {
		this.flushTimeout = flushTimeout;
		this.timerId = timerId;
		this.flushTask = new TimerTask() {

			@Override
			public void run() {
				flushTimeout();
				resetTimer();
			}
		};

		// schedule the timer for the first time
		resetTimer();
	}

	/**
	 * This method resets the flush timer. After calling this method the timer starts to count again and will eventually
	 * call {@link #flushTimeout()} after the timeout milliseconds.. if this method doesn't get called before this
	 * happens.
	 * 
	 * The method <b>thread safe</b>. It is synchronized internally and can be called from multiple threads
	 * concurrently.
	 */
	protected void resetTimer() {
		synchronized (this.flushTask) { // timer could be null, so synchronize on the task
			if (this.flushTimer != null) {
				// cancel the already scheduled timer
				this.flushTimer.cancel();
			}
			this.flushTimer = new Timer("Flush timer " + this.timerId);
			this.flushTimer.schedule(this.flushTask, this.flushTimeout);

		}

	}

	/**
	 * This method will be called by the flush timer, when the timeout has expired. The timer for the timeout can be
	 * reset using the {@link #resetTimer()} method.
	 */
	protected abstract void flushTimeout();

	

	/**
	 * This class wraps the elements that have to be stored by the ElasticPriorityQueue. It keeps the reference to the
	 * element and attaches the current system timestamp to is. This time stamp is used to determine the time for which
	 * a element has to be kept in cache.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 * 
	 */
	protected final class ElemWrap implements Serializable {
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
	protected final class ElemWrapComparator implements Comparator<ElemWrap>, Serializable {

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
			if (o1 == null && o2 == null)
				return 0;
			if (o1 == null && o2 != null)
				return 1;
			if (o1 != null && o2 == null)
				return -1;
			return this.comparator.compare(o1.element, o2.element);
		}

	}

	/**
	 * The callback is used to deal with the asychronosity introduced with the timeout timers. Whenever a subclass of
	 * {@link AbstractFlushQueue} is instantiated, a timer thread will be started in order to flush the queue out
	 * afrer a certain timeout has expired.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 * 
	 * @param <M>
	 *            the elements that will be passed in the process method
	 */
	protected interface ProcessCallback<M> {
		/**
		 * This method will be called from within the offer method or when the flush timeout has expired.
		 * 
		 * <b>Please Note:</b> This method will be called from multiple threads and therefore <b>needs to be thread
		 * safe</b>. The easiest way to achieve this is to synchonize the method.
		 * 
		 * @param elements
		 *            the elements that are now eligible for processing.
		 */
		void process(Collection<M> elements);
	}

}
