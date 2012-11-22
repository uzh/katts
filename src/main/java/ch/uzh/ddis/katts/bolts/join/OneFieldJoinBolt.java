package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractStreamInternSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.persistence.Storage;
import ch.uzh.ddis.katts.persistence.StorageFactory;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This Bolt joins two streams on a single variable (field) at the same time
 * with a certain precision.
 * 
 * 
 * @author Thomas Hunziker
 * 
 */
public class OneFieldJoinBolt extends AbstractSynchronizedBolt {

	private static final long serialVersionUID = 1L;
	private OneFieldJoinConfiguration configuration;
	private Storage<Object, Map<StreamConsumer, PriorityQueue<Event>>> buffers;

	private Logger logger = LoggerFactory.getLogger(OneFieldJoinBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		try {
			String boltId = this.getId();
			buffers = StorageFactory.createDefaultStorage(boltId + "_buffers");

		} catch (InstantiationException e) {
			logger.error("Could not load storage object.", e);
		} catch (IllegalAccessException e) {
			logger.error("Could not load storage object.", e);
		}
	}

	@Override
	public void execute(Event event) {
		event.getEmittedOn();
		Object joinOn = event.getVariableValue(this.getConfiguration().getJoinOn());

		// This will probably not work for anything but string values and even
		// with string values we're in danger
		// of working with (http://en.wikipedia.org/wiki/String_interning)
		// strings, which would not provide
		// thread safety.
		synchronized (joinOn) {

			Map<StreamConsumer, PriorityQueue<Event>> variableBuffer = buffers.get(joinOn);

			if (variableBuffer == null) {
				variableBuffer = new HashMap<StreamConsumer, PriorityQueue<Event>>();
				for (StreamConsumer consumer : this.getStreamConsumer()) {
					variableBuffer.put(consumer, new PriorityQueue<Event>());
				}
				buffers.put(joinOn, variableBuffer);
			}

			PriorityQueue<Event> streamBuffer = variableBuffer.get(event.getEmittedOn());

			streamBuffer.add(event);
			ack(event);

			joinEventsInBuffers(variableBuffer, event);

			reduceSizeOfAllOversizedQueues(variableBuffer);
		}
	}

	/**
	 * This method tries to find events to join in the queues and emit them on
	 * the output stream.
	 */
	private void joinEventsInBuffers(Map<StreamConsumer, PriorityQueue<Event>> variableBuffer, Event anchorEvent) {

		evictUnjoinableEvents(variableBuffer);

		long precision = this.getConfiguration().getJoinPrecision();

		List<Event> peekEvents = new ArrayList<Event>();
		for (Entry<StreamConsumer, PriorityQueue<Event>> entry : variableBuffer.entrySet()) {

			// When one queue is empty, no join can be done
			if (entry.getValue().size() <= 0) {
				return;
			}

			peekEvents.add(entry.getValue().peek());
		}

		// Verify that all the events in the peek list, are in the same time
		// range. The evictUnjoinableEvents must ensure this, but for confidence
		// we check it again:
		long startDate = peekEvents.get(0).getStartDate().getTime();
		for (Event event : peekEvents) {
			if (event.getStartDate().getTime() < (startDate - precision)
					|| event.getStartDate().getTime() > (startDate + precision)) {
				throw new RuntimeException("The join could not be processed, since some events get out of order. "
						+ "Potentially the OneFieldJoinBolt.evictUnjoinableEvents() failed somehow.");
			}
		}

		emitJoinEvents(peekEvents, anchorEvent);

		// Remove the peek events from the queues
		for (Entry<StreamConsumer, PriorityQueue<Event>> entry : variableBuffer.entrySet()) {
			entry.getValue().poll();
		}

		// It is possible that other events can be joined. So
		// try again. The stop condition of the recursion is that
		// on of the queues is empty.
		joinEventsInBuffers(variableBuffer, anchorEvent);

	}

	/**
	 * This method emits a joined variable binding of all events given in the
	 * events parameter.
	 * 
	 * @param events
	 *            all events that have been joined together in this time step.
	 * @param anchorEvent
	 *            storm forces us to re-use one of the events this bolt
	 *            received. We attach all our new variable bindings to this
	 *            event before sending it on.
	 */
	private void emitJoinEvents(List<Event> events, Event anchorEvent) {
		for (Stream stream : this.getStreams()) {
			VariableBindings bindings = getEmitter().createVariableBindings(stream, anchorEvent);

			// copy all varables from the input events into the new bindings
			// variable which we emit from this bolt.
			for (Event event : events) {
				for (Variable variable : event.getVariables()) {
					bindings.add(variable.getName(), event.getVariableValue(variable));
				}
			}
			bindings.setStartDate(events.get(0).getStartDate());
			bindings.setEndDate(events.get(0).getEndDate());

			bindings.emit();

		}
	}

	/**
	 * This method removes all events from the buffer, that can never be joined,
	 * because the required events on the other streams can never appear.
	 * 
	 * @param variableBuffer
	 */
	private void evictUnjoinableEvents(Map<StreamConsumer, PriorityQueue<Event>> variableBuffer) {
		long precision = this.getConfiguration().getJoinPrecision();
		for (Entry<StreamConsumer, PriorityQueue<Event>> entry : variableBuffer.entrySet()) {
			PriorityQueue<Event> queue = entry.getValue();

			if (queue.size() > 0) {
				Event firstEvent = queue.peek();
				for (Entry<StreamConsumer, PriorityQueue<Event>> innerEntry : variableBuffer.entrySet()) {
					if (innerEntry.getValue().size() > 0 && innerEntry.getKey() != entry.getKey()) {
						if (firstEvent.getStartDate().getTime() < entry.getValue().peek().getStartDate().getTime()
								- precision) {
							// Since the event in the inner loop is greater then
							// we can be sure, that the outer even
							// can be never joined with all the events in all
							// queues.
							queue.poll();
							evictUnjoinableEvents(variableBuffer);
							return;
						}
					}
				}

			}
		}

	}

	/**
	 * This method reduce the size of all queues to the maximal allowed size.
	 * 
	 * @param variableBuffer
	 */
	private void reduceSizeOfAllOversizedQueues(Map<StreamConsumer, PriorityQueue<Event>> variableBuffer) {
		int max = this.getConfiguration().getMaxBufferSize();
		for (Entry<StreamConsumer, PriorityQueue<Event>> entry : variableBuffer.entrySet()) {
			PriorityQueue<Event> queue = entry.getValue();
			if (queue.size() > max) {
				while (queue.size() > max) {
					queue.poll();
				}
			}
		}
	}

	@Override
	public String getId() {
		return getConfiguration().getId();
	}

	public OneFieldJoinConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(OneFieldJoinConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public String getSynchronizationDateExpression() {
		// TODO Auto-generated method stub
		return null;
	}

}
