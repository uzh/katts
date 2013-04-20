package ch.uzh.ddis.katts.bolts;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.utils.ElasticPriorityQueue;

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
	private final ElasticPriorityQueue<Event> buffer;

	/** Events will be kept in the buffer for this many milliseconds, before processing them in temporal order. */
	private final long bufferDelay;

	/**
	 * Because we cannot bank on new events coming in, we create a timer that drains the buffer when the delay expired.
	 */
	private Timer drainTimer;

	/**
	 * Default constructor that uses a delay of two seconds. Why two? I'll tell you why: I don't know!
	 */
	public AbstractSynchronizedBolt() {
		this(2000);
	}

	/**
	 * Creates a synchronized bolt that keeps its entries in the buffer for bufferDelay milliseconds before processing
	 * them in temporal order.
	 * 
	 * @param bufferDelay
	 *            the number of milliseconds, an event will be kept in the buffer.
	 */
	public AbstractSynchronizedBolt(int bufferDelay) {
		this.bufferDelay = bufferDelay;
		this.buffer = new ElasticPriorityQueue<Event>(this.bufferDelay, new Event.EndDateComparator());
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		drainTimer = new Timer("Drain timer " + context.getThisTaskId());
		drainTimer.schedule(new TimerTask() {
			@Override
			public void run() {
				synchronized (AbstractSynchronizedBolt.this.buffer) {
					for (Event orderedEvent : AbstractSynchronizedBolt.this.buffer.drainElements()) {
						execute(orderedEvent);
					}
				}
			}
		}, 0, this.bufferDelay); // drai (and execute) all events at least every "bufferDelay" milliseconds
	}

	@Override
	public void executeRegularTuple(Tuple input) {
		Event event = createEvent(input);
		/*
		 * Storm uses the same thread to call execute. However, since we drain the buffer using a timer (which is a
		 * second thread) we need so synchronize access to it.
		 */
		synchronized (buffer) {
			for (Event orderedEvent : this.buffer.offer(event)) {
				execute(orderedEvent);
			}
		}
	}

	@Override
	public abstract void execute(Event event);

	public void ack(Event event) {
		ack(event.getTuple());
	}

}
