package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.persistence.Storage;
import ch.uzh.ddis.katts.persistence.StorageFactory;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This abstract bolt guarantees that the events inside a stream are synchronized. The different streams are not
 * synchronized.
 * 
 * @author Thomas Hunziker
 * 
 */
public abstract class AbstractSynchronizedBolt extends AbstractBolt {

	private static final long serialVersionUID = 1L;

	private Storage<StreamConsumer, PriorityQueue<Event>> buffers;

	private Storage<StreamConsumer, HashMap<Integer, Date>> lastDatePerTask;

	private Storage<StreamConsumer, Date> lastDateProcessed;

	private Logger logger = LoggerFactory.getLogger(AbstractSynchronizedBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		try {
			String boltId = this.getId();
			buffers = StorageFactory.createDefaultStorage(boltId + "_buffers");
			lastDatePerTask = StorageFactory.createDefaultStorage(boltId + "_last_date_per_task");
			lastDateProcessed = StorageFactory.createDefaultStorage(boltId + "_last_date_processed");

		} catch (InstantiationException e) {
			logger.error("Could not load storage object.", e);
		} catch (IllegalAccessException e) {
			logger.error("Could not load storage object.", e);
		}

		for (StreamConsumer stream : this.getStreamConsumer()) {
			buffers.put(stream, new PriorityQueue<Event>(20, new EventTimeComparator()));
		}
	}

	/**
	 * Returns a unique id for this bolt.
	 * 
	 * @return
	 */
	public abstract String getId();

	@Override
	public void execute(Tuple input) {
		Event event = createEvent(input);
		PriorityQueue<Event> buffer = buffers.get(event.getEmittedOn());
		Date lastDate = lastDateProcessed.get(event.getEmittedOn());

		if (lastDate != null && event.getStartDate().before(lastDate)) {
			// TODO: Log the case, that the event is out of order and can not be processed
		} else {
			buffer.add(event);
			updateBufferDates(event);
			super.ack(event);
		}

		executeNextEventInBuffer(buffer, event.getEmittedOn());
	}

	@Override
	public void ack(Event event) {
		// we do not need to call the super
		// method because we ack the event already
		// when we write it to the buffer.

		PriorityQueue<Event> buffer = buffers.get(event.getEmittedOn());
		buffer.remove(event);
		executeNextEventInBuffer(buffer, event.getEmittedOn());
	}

	private void executeNextEventInBuffer(PriorityQueue<Event> buffer, StreamConsumer stream) {
		// TODO: Check if we need to do some thread synchronization

		Event next = buffer.peek();

		boolean bufferTimeout = false;
		long timeout = stream.getRealBufferTimout();
		if (timeout > 0) {
			Date lastDate = lastDateProcessed.get(next.getEmittedOn());

			if (Math.abs((lastDate.getTime() - next.getStartDate().getTime())) > timeout) {
				bufferTimeout = true;
			}
		}

		if (next != null && (isInSequence(next) || bufferTimeout)) {
			lastDateProcessed.put(next.getEmittedOn(), next.getStartDate());
			executeSynchronizedEvent(next);
		}
	}

	/**
	 * This method is called when ever a event is ready for execution. The events are ordered in the stream internal
	 * order. This method helps overriding classes to control further processing of the events.
	 * 
	 * @param event
	 */
	protected void executeSynchronizedEvent(Event event) {
		execute(event);
	}

	private boolean isInSequence(Event event) {
		event.getEmittedOn();
		int taskId = event.getTuple().getSourceTask();
		HashMap<Integer, Date> streamMap = lastDatePerTask.get(event.getEmittedOn());

		for (Entry<Integer, Date> set : streamMap.entrySet()) {
			if (set.getKey() != taskId) {
				if (set.getValue() != null && set.getValue().before(event.getStartDate())) {
					// If there is a event from another task, which is
					// before the actual date, then it is not in sequence.
					return false;
				}
			}
		}

		return true;
	}

	private void updateBufferDates(Event event) {
		HashMap<Integer, Date> streamMap = lastDatePerTask.get(event.getEmittedOn());
		if (streamMap == null) {
			streamMap = new HashMap<Integer, Date>();
			lastDatePerTask.put(event.getEmittedOn(), streamMap);
		}
		Date lastDate = streamMap.get(event.getTuple().getSourceTask());
		streamMap.put(event.getTuple().getSourceTask(), lastDate);
	}

}
