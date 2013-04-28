package ch.uzh.ddis.katts.bolts;

import java.util.Collection;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.utils.AbstractFlushQueue.ProcessCallback;
import ch.uzh.ddis.katts.utils.SortedTimeoutBuffer;

/**
 * This abstract bolt implementation provides synchronization of incoming events to the subclasses.
 * 
 * This implementation uses a an instance of {@link ElasticPriorityQueue} to do the synchronization. Elements will be
 * kept in the buffer for a minimal delay. After that period, they are processed in temporal order. See {@see
 * ElasticPriorityQueue} for more details on how the temporal ordering works.
 * 
 * @author Thomas Hunziker
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 * @see ElasticPriorityQueue
 */
public abstract class AbstractSynchronizedBolt extends AbstractVariableBindingsBolt {

	/** This datastructure does the temporal ordering. */
	private SortedTimeoutBuffer<Event> buffer;

	/**
	 * Events will be kept in the buffer at least for this many milliseconds, before processing them in temporal order.
	 * This allows for small irregularities in the ordering within each incoming stream.
	 */
	private final long bufferTimeout;

	/**
	 * After this many milliseconds, an incoming stream will be assumed to have no more elements, so processing goes on.
	 * This is to guard against stalling in case of a filter bolt stopping to send anything.
	 */
	private final long waitTimeout;

	/**
	 * Default constructor that uses a delay of two seconds. Why two? I'll tell you why: I don't know!
	 */
	public AbstractSynchronizedBolt() {
		this(1000, 2000);
	}

	/**
	 * Creates a synchronized bolt that keeps its entries in the buffer for at most maxDelay milliseconds before
	 * processing them in temporal order. If there are tuples from all incoming streams before maxDelay has expired, all
	 * tuples that have a date older than the oldest date from all streams, will be processed.
	 * 
	 * @param bufferTimeout
	 *            the number of milliseconds, an event will be kept in the buffer at least. This allows for small
	 *            irregularities in the ordering within each incoming stream.
	 * @param waitTimeout
	 *            the tolerance towards stream interruptions, i.e. the number of milliseconds we will wait before
	 *            declaring that a stream is not sending anything anymore.
	 */
	public AbstractSynchronizedBolt(int bufferTimeout, int waitTimeout) {
		this.bufferTimeout = bufferTimeout;
		this.waitTimeout = waitTimeout;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		ProcessCallback<Event> callback = new ProcessCallback<Event>() {

			@Override
			public void process(Collection<Event> elements) {
				/* This method might be called from multiple threads concurrently */
				synchronized (AbstractSynchronizedBolt.this) {
					for (Event orderedEvent : elements) {
						execute(orderedEvent);
					}
				}
			}
		};

		this.buffer = new SortedTimeoutBuffer<Event>(context.getThisStreams(), //
				this.bufferTimeout, //
				this.waitTimeout, //
				new Event.EndDateComparator(), callback);

	}

	@Override
	public void executeRegularTuple(Tuple input) {
		this.buffer.offer(createEvent(input), input.getSourceStreamId());
	}

	@Override
	public abstract void execute(Event event);

	public void ack(Event event) {
		ack(event.getTuple());
	}

}
