package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
public abstract class AbstractStreamInternSynchronizedBolt extends AbstractVariableBindingsBolt {

	private static final long serialVersionUID = 1L;

	private Storage<StreamConsumer, PriorityQueue<Event>> buffers;

	private Storage<StreamConsumer, HashMap<Integer, Date>> lastDatePerTask;

	private Storage<StreamConsumer, Date> lastDateProcessed;

	private Logger logger = LoggerFactory.getLogger(AbstractStreamInternSynchronizedBolt.class);
	
	private TopologyContext context;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		this.context = context;
		
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
	public void executeHeartBeatFreeTuple(Tuple input) {
		
		Event event = createEvent(input);
		
		String streamId = event.getEmittedOn().getStream().getId().intern();
		
		// Synchronize by the incoming stream
		synchronized(streamId) {
			PriorityQueue<Event> buffer = buffers.get(event.getEmittedOn());
			
			Date lastDate = lastDateProcessed.get(event.getEmittedOn());
			if (lastDate != null && event.getStartDate().before(lastDate)) {
				long timeout = event.getEmittedOn().getRealBufferTimout();
				logger.info(String.format("An event was out of order and it was thrown away. Timeout is set to: %1d. Component ID: %1s", timeout, this.getId()));
			} else {
				buffer.add(event);
				updateBufferDates(event);
				super.ack(event);
			}
			
			executeNextEventInBuffer(buffer, event.getEmittedOn());
		}
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

		Event next = buffer.peek();
		
		if (next != null) {
			boolean bufferTimeout = false;
			long timeout = stream.getRealBufferTimout();
			if (timeout > 0) {
				Date lastDate = lastDateProcessed.get(next.getEmittedOn());

				if (lastDate != null && Math.abs((lastDate.getTime() - next.getEndDate().getTime())) > timeout) {
					bufferTimeout = true;
				}
				else if (lastDate == null) {
					bufferTimeout = true;
				}
			}

			if(isInSequence(next) || bufferTimeout) {
				lastDateProcessed.put(next.getEmittedOn(), next.getStartDate());
				executeSynchronizedEvent(next);
			}
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
		int currentTaskId = event.getTuple().getSourceTask();
		HashMap<Integer, Date> streamMap = lastDatePerTask.get(event.getEmittedOn());

//		List<Integer> taskList = context.getComponentTasks(event.getTuple().getSourceComponent());
		// It would be a good approach to check against all tasks, that may send data to this bolt. The
		// problem is, that it is not clear if all task will send the data to this or to another bolt.
		// So we need to iterate over all known tasks that send data to this bolt.
		for (Entry<Integer, Date> entry : streamMap.entrySet()) {
			Integer taskId = entry.getKey();
			if (taskId != currentTaskId) {
				
				Date lastEventDate = entry.getValue();
				if (lastEventDate != null && lastEventDate.before(event.getStartDate())) {
					return false;
				}
			}
		}	

		return true;
	}

	private void updateBufferDates(Event event) {
		HashMap<Integer, Date> streamMap = lastDatePerTask.get(event.getEmittedOn());
		Integer sourceTaskId = event.getTuple().getSourceTask();
		if (streamMap == null) {
			streamMap = new HashMap<Integer, Date>();
		}
		
		// TODO: Use some expression to determine the synchronization time
		Date lastDate = event.getEndDate();
		streamMap.put(sourceTaskId, lastDate);
		
		lastDatePerTask.put(event.getEmittedOn(), streamMap);
	}

}
