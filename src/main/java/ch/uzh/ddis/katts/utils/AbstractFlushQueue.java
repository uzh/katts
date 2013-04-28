package ch.uzh.ddis.katts.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
	 * For performance reasons we do not actually reset the timer each time the {@link #resetTimer()} method is called.
	 * We only update this field to represent the current system time, when the last reset has happened. Whenever the
	 * scheduled timer times out, we reschedule it for (flushTimeout - (currentTime - lastReset)) milliseconds. This
	 * will essentially shorten down the next timeout interval by the time that elapsed between the last timer reset and
	 * now.
	 */
	private AtomicLong lastReset = new AtomicLong(0);

	/**
	 * Creates a new elastic priority queue with the given delay.
	 * 
	 * @param flushTimeout
	 *            the timeout (milliseconds) after which the queue flushed. If this value is 0, no timer will be
	 *            started.
	 */
	public AbstractFlushQueue(final long flushTimeout) {
		if (flushTimeout > 0) {
			final ScheduledThreadPoolExecutor executor;
			final Runnable flushTask;

			resetTimer(); // set reset date to now

			executor = new ScheduledThreadPoolExecutor(1);
			flushTask = new Runnable() {
				private long lr = AbstractFlushQueue.this.lastReset.get();

				@Override
				public void run() {
					// only call the flushTimeout method, if the timer has not been "reset" in the meantime
					if (lr == AbstractFlushQueue.this.lastReset.get()) {
						flushTimeout();
					} else {
						lr = AbstractFlushQueue.this.lastReset.get();
					}
					scheduleFlushTask(this, executor, flushTimeout); // re-schedule
				}
			};

			scheduleFlushTask(flushTask, executor, flushTimeout); // schedule for the first time
		}
	}

	private void scheduleFlushTask(Runnable task, final ScheduledThreadPoolExecutor executor, long flushTimeout) {
		executor.schedule(task, flushTimeout - (System.currentTimeMillis() - this.lastReset.get()),
				TimeUnit.MILLISECONDS);
	}

	/**
	 * This method resets the flush timer. After calling this method the timer starts to count again and will eventually
	 * call {@link #flushTimeout()} after the timeout milliseconds, unless this method doesn't get called again before
	 * this happens.
	 * 
	 * The method is <b>thread safe</b> and can be called from multiple threads concurrently.
	 */
	protected void resetTimer() {
		// we do not actually reset the timer here, see #lastReset
		this.lastReset.set(System.currentTimeMillis());
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
		public final E element;
		public final long timestamp;

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
	 * {@link AbstractFlushQueue} is instantiated, a timer thread will be started in order to flush the queue out afrer
	 * a certain timeout has expired.
	 * 
	 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
	 * 
	 * @param <M>
	 *            the elements that will be passed in the process method
	 */
	public interface ProcessCallback<M> {
		// TODO: if we have full control over this, we can synchronize calls of this method ourselves and remove the
		// the warning.
		/**
		 * This method will be called from within the offer method or when the flush timeout has expired.
		 * <p/>
		 * <b>Please Note:</b> This method will be called from multiple threads and therefore <b>needs to be thread
		 * safe</b>. The easiest way to achieve this is to synchonize the method.
		 * 
		 * @param elements
		 *            the elements that are now eligible for processing.
		 */
		void process(Collection<M> elements);
	}

}
