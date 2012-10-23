package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This bolt implements a generic synchronization between all receiving streams of the bolt. The synchronization of the
 * streams provides a single stream of events in a correct temporal order.
 * 
 * @author Thomas Hunziker
 * 
 */
public abstract class AbstractStreamSynchronizedBolt extends AbstractSynchronizedBolt {

	private static final long serialVersionUID = 1L;

	// TODO: Move this buffer into the storage
	private PriorityQueue<StreamSynchronizedEventWrapper> buffer = new PriorityQueue<StreamSynchronizedEventWrapper>();

	// TODO: Move this lastDatePerStream into the storage
	private Map<StreamConsumer, Date> lastDatePerStream = new HashMap<StreamConsumer, Date>();

	private Logger logger = LoggerFactory.getLogger(AbstractStreamSynchronizedBolt.class);

	private Expression eventSynchronizationExpression;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		// try {
		// String boltId = this.getId();
		// lastDatePerStream = StorageFactory.createDefaultStorage(boltId + "_last_date_per_stream");
		//
		// } catch (InstantiationException e) {
		// logger.error("Could not load storage object.", e);
		// } catch (IllegalAccessException e) {
		// logger.error("Could not load storage object.", e);
		// }

		// for (StreamConsumer stream : this.getStreamConsumer()) {
		// buffers.put(stream, new PriorityQueue<Event>(stream.getMaxBufferSize(), new EventTimeComparator()));
		// }

	}

	/**
	 * This method is called when the event is synchronized with in the stream. We override this method to provide
	 * stream synchronization.
	 */
	@Override
	protected void executeSynchronizedEvent(Event event) {

		StreamSynchronizedEventWrapper syncEvent = new StreamSynchronizedEventWrapper(event,
				this.getSynchronizationDate(event));
		buffer.add(syncEvent);
		lastDatePerStream.put(event.getEmittedOn(), syncEvent.getSynchronizationDate());
		event.ack();
		executeEventsInBuffer();

	}

	/**
	 * This method executes all events in the buffer, with the correct temporal order.
	 */
	private void executeEventsInBuffer() {
		StreamSynchronizedEventWrapper next = buffer.peek();
		if (next != null) {
			boolean bufferTimeout = false;
			long timeout = next.getEmittedOn().getRealBufferTimout();
			if (timeout > 0) {
				Date lastDate = lastDatePerStream.get(next.getEmittedOn());

				if (Math.abs((lastDate.getTime() - next.getSynchronizationDate().getTime())) > timeout) {
					bufferTimeout = true;
				}
			}

			if ((isInTemporalOrder(next) || bufferTimeout)) {
				executeSynchronizedStreamEvent(next);
			}
		}
	}

	@Override
	public void ack(Event event) {
		super.ack(event);

		buffer.remove(event);

		// Since we found an event with correct temporal order, try to find
		// other events and execute them.
		executeEventsInBuffer();
	}

	/**
	 * This method checks if the given event is in the correct temporal order corresponding to the all streams. An event
	 * is in temporal order, when it has the smallest event date overall streams.
	 * 
	 * @param event
	 *            The event to check for ordering.
	 * @return True when the event is in Order, False when the event is not in the corresponding order.
	 */
	private boolean isInTemporalOrder(StreamSynchronizedEventWrapper event) {

		for (Entry<StreamConsumer, Date> dateEntry : lastDatePerStream.entrySet()) {
			if (dateEntry.getValue().before(event.getSynchronizationDate())) {
				return false;
			}
		}

		return true;
	}

	/**
	 * This method is called whenever the event is synchronized with the different streams. This method can be
	 * overridden by subclasses.
	 * 
	 * @param event
	 */
	protected void executeSynchronizedStreamEvent(Event event) {
		execute(event);
	}

	/**
	 * This method returns the date on which the event should be synchronized with other events on other streams. The
	 * date is determine by an SpEL expression (
	 * {@link AbstractStreamSynchronizedBolt#getSynchronizationDateExpression()}).
	 * 
	 * @param event
	 *            The event to be synchronized.
	 * @return The date on which the synchronization should be done
	 */
	protected Date getSynchronizationDate(Event event) {
		setupSynchronizationDateExpression();

		StandardEvaluationContext context = new StandardEvaluationContext();
		context.setVariable("event", event);

		return (Date) eventSynchronizationExpression.getValue(context);
	}

	/**
	 * This method setups the SpEL expression for determine the synchronization date.
	 */
	private void setupSynchronizationDateExpression() {
		if (eventSynchronizationExpression == null) {
			ExpressionParser parser = new SpelExpressionParser();
			eventSynchronizationExpression = parser.parseExpression(this.getSynchronizationDateExpression());
		}
	}

	/**
	 * This method should return an expression which determines the date on which the event synchronization is done. The
	 * one and only variable available is "event". It has the type {@link Event}.
	 * 
	 * @return The expression for determine the synchronization date.
	 */
	public abstract String getSynchronizationDateExpression();

}
