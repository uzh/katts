package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This Bolt joins two streams on a single variable (field) at the same time with a certain precision. This
 * implementation provides a better performance as the temporal join, because the conditions and eviction rules are
 * directly encoded in Java. Additionally the buffers can be emptied more efficent as it is possible with the temporal
 * join.
 * 
 * This bolt has a state and hence is stateful.
 * 
 * @author Thomas Hunziker
 * 
 */
public class OneFieldJoinBolt extends AbstractSynchronizedBolt {

	private static final long serialVersionUID = 1L;
	private OneFieldJoinConfiguration configuration;

	/**
	 * The buffers contains the data that is on hold, because we wait for another stream. The data inside the buffer is
	 * ordered by the end date of the event.
	 */
	// TODO Move the buffers to the central storage infrastructure.
	private Map<Object, Map<StreamConsumer, Event>> buffers;
	private Collection<StreamConsumer> streamList;

	private Date lastProcessedDate;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		streamList = this.getStreamConsumer();
	}

	@Override
	public synchronized void execute(Event event) {

		long precision = this.getConfiguration().getJoinPrecision();
		long currentEndDate = event.getEndDate().getTime();
		boolean joinable = true;

		// TODO Add the precision on the join. This requires the refactoring of the buffer, because for each slot a
		// different buffer is required.

		// Truncate the buffer, when we are sure to be continued in time. This is important, because not evicted data
		// may not removed and the memory consumption increases rapidly. Especial in case two streams from the triple
		// filter are joined, because the key is for each variable binding changed.
		if (lastProcessedDate == null || lastProcessedDate.before(event.getEndDate())) {
			buffers = new HashMap<Object, Map<StreamConsumer, Event>>();
		}

		Object joinOn = event.getVariableValue(this.getConfiguration().getJoinOn());

		Map<StreamConsumer, Event> joinBuffer = buffers.get(joinOn);

		if (joinBuffer == null) {
			joinBuffer = new ConcurrentHashMap<StreamConsumer, Event>();
			this.buffers.put(joinOn, joinBuffer);
		}

		// Remove all events from join buffer, that are before the current event.
		List<StreamConsumer> toRemove = new ArrayList<StreamConsumer>();
		for (Entry<StreamConsumer, Event> entry : joinBuffer.entrySet()) {
			if (entry.getValue().getEndDate().getTime() < (currentEndDate - precision)) {
				toRemove.add(entry.getKey());
			}
		}

		for (StreamConsumer stream : toRemove) {
			joinBuffer.remove(stream);
		}

		joinBuffer.put(event.getEmittedOn(), event);

		// Check if we can emit a joined tuple
		for (StreamConsumer consumer : streamList) {
			Event streamEvent = joinBuffer.get(consumer);

			if (streamEvent == null) {
				joinable = false;
				break;
			}
		}

		if (joinable) {
			emitJoinEvents(joinBuffer, event);
			buffers.remove(joinOn);
		}

		lastProcessedDate = event.getEndDate();
		ack(event);
	}

	/**
	 * This method emits a joined variable binding of all events given in the events parameter.
	 * 
	 * @param events
	 *            all events that have been joined together in this time step.
	 * @param anchorEvent
	 *            storm forces us to re-use one of the events this bolt received. We attach all our new variable
	 *            bindings to this event before sending it on.
	 */
	private void emitJoinEvents(Map<StreamConsumer, Event> events, Event anchorEvent) {
		for (Stream stream : this.getStreams()) {
			VariableBindings bindings = getEmitter().createVariableBindings(stream, anchorEvent);

			// copy all variables from the input events into the new bindings
			// variable which we emit from this bolt.
			for (Entry<StreamConsumer, Event> entry : events.entrySet()) {
				for (Variable variable : entry.getValue().getVariables()) {
					bindings.add(variable.getName(), entry.getValue().getVariableValue(variable));
				}
			}
			bindings.setStartDate(anchorEvent.getStartDate());
			bindings.setEndDate(anchorEvent.getEndDate());

			bindings.emit();
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

}
