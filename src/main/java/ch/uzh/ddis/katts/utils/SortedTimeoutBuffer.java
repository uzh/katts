/**
 * 
 */
package ch.uzh.ddis.katts.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

// TODO lorenz: should the bufferTimeout always be added to the waitTimeout?

/**
 * This buffer sorts elements according to their semantic ordering (most often time) and buffers its input for a minimal
 * time. The semantic ordering is defined using a comparator that needs to be supplied at construction time. The buffer
 * supports multiple input channels and keeps track of the position in the semantic ordering that each of its input
 * channels has. The user of this class needs to provide a callback object whose process method will be called whenever
 * items are "eligible" for processing.
 * <p>
 * Items can be eligible for processing if the following three conditions are met:
 * <ol>
 * <li>We have received at least one item from each of the input channels. This condition makes sure that we can handle
 * situations in which one channel is a lot faster than all the other channels.</li>
 * <li>A configurable minimum of time (buffer timeout) needs to pass after the item has been added to the buffer. This
 * is to provide robustness against small anomalies in the order of the input streams in each source.</li>
 * <li>The second condition allows us to assume that the order of elements within each channel is strictly increasing.
 * In addition to the the first two conditions, elements need to satisfy one of the following two sub-conditions in
 * order to be eligible for processing:
 * <ol>
 * <li>If the semantic order of an element is smaller or equal to the minimum semantic position of all input channels.
 * For example if we have two channels one being at position 10 and the other being at position 12, all elements &lt;=
 * 10 would be eligible for processing.</li>
 * <li>In order to prevent stalling (i.e. one channel stops sending data and hence blocks the processing of data from
 * all other channels, we use a maximum wait timeout after which elements can be processed even if their semantic
 * ordering is greater than the minimum semantic position of all input channels.</li>
 * </ol>
 * </li>
 * </ol>
 * 
 * This buffer starts a timeout timer task that will make sure that items get sent to the callback object, even after no
 * more elements are added to the buffer. This timer will be scheduled to be run after the greater of the two timeouts
 * of the most recently added element has expired (max(waitTimeout, bufferTimeout)).
 * 
 * <p/>
 * 
 * This class is <b>not thread safe</b>, so access to it has to be synchronized.
 * 
 * @param <E>
 *            The type of element being buffered. <b>Please note:</b> This class makes use of the equals() and
 *            hashCode() method of these elements to do its housekeeping. Therefore, all elements that are being stored
 *            in this buffer need to have solid implementations of the equals and hashCode methods.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SortedTimeoutBuffer<E> extends AbstractFlushQueue<E> implements Serializable {

	/** This is the backing queue that takes care of all the ordering. */
	private PriorityQueue<ElemWrap> backingQueue;

	/**
	 * This is the delay (milliseconds) during which elements are kept in the cache before they are returned.
	 */
	private long bufferTimeout;

	/**
	 * This is the maximum wait time (in milliseconds) for which we will wait for input on all incoming channels. If
	 * there was no input on any channel for this long, we will process the elements in the queue, even if their
	 * semantic ordering is greater than the minimal current semantic position on all channels.
	 */
	private long waitTimeout;

	/** The comparator used to compare elements processed by this queue. */
	private Comparator<E> elementComparator;

	/** The number of incoming channels we expect messages on. */
	private final int numChannels;

	/**
	 * In this map, we store a reference to the semantically "highest/latest" element we received on each channel. All
	 * these elements will also be stored in the {@link #smallestElementFirst} queue.
	 */
	private Map<String, E> mostRecentElementOfChannel;

	/**
	 * This queue contains one element for each channel. By peeking at the first element, we know how far we can process
	 * all other elements of the cache. /*
	 */
	private PriorityQueue<E> smallestElementFirst;

	/** We call the process method of this object, for all elements that are eligible for processing. */
	private ProcessCallback<E> callback;

	/**
	 * Creates a new buffer object.
	 * 
	 * @param inputChannels
	 *            a collection that contains an itentifier for each channel, this buffer will receive elements for.
	 * @param bufferTimeout
	 *            a minimal time (in milliseconds) for which all elements will be kept in the buffer before processing.
	 * @param waitTimeout
	 *            the maximum time (in milliseconds) for which this buffer waits, before the an element is processed
	 *            even if the semantic order of the element is greater than the minimum order of all input channels.
	 * @param comparator
	 *            the semantic ordering of the elements will be determined using this comparator.
	 * @param callback
	 *            the object whose process method will be called for the elements that are eligible for processing.
	 */
	public SortedTimeoutBuffer(Collection<String> inputChannels, long bufferTimeout, long waitTimeout,
			Comparator<E> comparator, ProcessCallback<E> callback) {
		super(Math.max(waitTimeout, bufferTimeout));

		if (inputChannels == null || comparator == null || callback == null) {
			throw new NullPointerException("Null values are not supported for any of the parameters.");
		}

		this.numChannels = inputChannels.size(); // for now we actually only need the number channels
		this.bufferTimeout = bufferTimeout;
		this.waitTimeout = waitTimeout;
		this.elementComparator = comparator;
		this.callback = callback;

		this.backingQueue = new PriorityQueue<ElemWrap>(1000, new ElemWrapComparator(comparator));
		this.mostRecentElementOfChannel = new HashMap<String, E>();
		this.smallestElementFirst = new PriorityQueue<E>(1000, this.elementComparator);
	}

	/**
	 * Adds e to the queue and calls the callback for all elements that are "eligible" for processing.
	 * 
	 * @param e
	 *            the element to add.
	 * @param channel
	 *            the channel the element was received on.
	 */
	public void offer(E e, String channel) {

		if (e == null) {
			throw new NullPointerException("Null is not supported.");
		}

		// housekeeping of channel collections
		this.smallestElementFirst.remove(this.mostRecentElementOfChannel.get(channel));
		this.smallestElementFirst.add(e);
		this.mostRecentElementOfChannel.put(channel, e);

		// wrap the element and add it to the queue
		synchronized (this.backingQueue) {
			this.backingQueue.offer(new ElemWrap(e));
		}
		resetTimer(); // since we just added something..
		processEligibleElements();
	}

	/**
	 * Searches for eligible elements and processed them using the callback's process method if there are any.
	 */
	protected void processEligibleElements() {
		ArrayList<E> eligibleElements = new ArrayList<E>();

		synchronized (this.backingQueue) { // make sure nobody adds anything to the queue while we're processing it
			while (!this.backingQueue.isEmpty()) {

				// Condition 1: elements on all channels
				if (this.smallestElementFirst.size() < numChannels) {
					//System.out.println(Thread.currentThread().getName() + ": cond 1");
					break;
				}

				ElemWrap preview = this.backingQueue.peek();
				//System.out.println(Thread.currentThread().getName() + ": preview: " + preview.element);
				// Condition 2: bufferTimeout
				if (preview.timestamp > System.currentTimeMillis() - this.bufferTimeout) {
					//System.out.println(Thread.currentThread().getName() + ": cond 2");
					break;
				}

				/*
				 * Condition 3:
				 * 
				 * If the current element (preview) is smaller or equal than the elements we have last receive on all
				 * channels, we can process it. Otherwise the waitTimeout will be used and we process the element if its
				 * timestamp is smaller than [now - waitTimeout].
				 */
				boolean cond31 = (this.elementComparator.compare(preview.element, this.smallestElementFirst.peek()) <= 0);
				boolean cond32 = (preview.timestamp <= System.currentTimeMillis() - this.waitTimeout);
				if (cond31 || cond32) {
					//System.out.println(Thread.currentThread().getName() + ": 31: " + cond31 + " 32: " + cond32);
					eligibleElements.add(this.backingQueue.poll().element);
				} else {
					//System.out.println(Thread.currentThread().getName() + ": cond 3");
					break; // no more elements should be returned
				}

			}
		}

		// process all eligible elements if there are any
		if (eligibleElements.size() > 0) {
			this.callback.process(eligibleElements);
		}
	}

	@Override
	protected void flushTimeout() {
		processEligibleElements();
	}

}
