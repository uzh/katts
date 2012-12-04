package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import backtype.storm.tuple.Tuple;

public abstract class AbstractSynchronizedBolt extends AbstractVariableBindingsBolt {

	private static final long serialVersionUID = 1L;

	private PriorityQueue<StreamSynchronizedEventWrapper> buffer = new PriorityQueue<StreamSynchronizedEventWrapper>();
	private HashMap<Integer, Date> lastDatePerTask = new HashMap<Integer, Date>();

	private Date lastDateProcessed;
	
	@Override
	public abstract void execute(Event event);

	@Override
	public void executeRegularTuple(Tuple input) {
		Event event = createEvent(input);
		StreamSynchronizedEventWrapper syncEvent = new StreamSynchronizedEventWrapper(event,
				this.getSynchronizationDate(event));
		
		synchronized (buffer) {
			buffer.add(syncEvent);
			lastDatePerTask.put(syncEvent.getTuple().getSourceTask(), syncEvent.getSynchronizationDate());
		}

//		this.ack(input);

		executeEventsInBuffer();
	}

	/**
	 * This method is called when ever a event is ready for execution. This method helps overriding classes to control
	 * further processing of the events.
	 * 
	 * @param event
	 */
	protected void executeSynchronizedEvent(Event event) {
		execute(event);
	}

	private void executeEventsInBuffer() {
		
		synchronized (buffer) {
			boolean stop = false;
			while (!stop) {
				StreamSynchronizedEventWrapper next = buffer.peek();
				if (next != null && isEventInTemporalOrder(next)) {
					executeSynchronizedEvent(next);
					buffer.remove(next);
				}
				else {
					stop = true;
				}
			}
		}
		
	}

	private boolean isEventInTemporalOrder(StreamSynchronizedEventWrapper event) {
		
		// We iterate over all possible tasks, that can send us events. If one of the task can send us a event, which
		// may have a event date before the event in question, then return false.
		for (Integer taskId : this.getAllSourceTasksFromIncomingStreams()) {

			Date eventDate = lastDatePerTask.get(taskId);
			HeartBeat heartBeat = this.getLastHeartBeatPerTask(taskId);

			// In case no heart beat and no event is present, we are at the very beginning and need to wait.
			if (eventDate == null && heartBeat == null) {
				return false;
			}

			// If there is a heart beat, that is after the event's date, then we can use this date.
			if (eventDate == null || (heartBeat != null && eventDate.before(heartBeat.getStreamDate()))) {
				eventDate = heartBeat.getStreamDate();
			}

			// Check if the given event is not before the last known date on a given task stream. This would break the
			// constraint, that another event can occur that is out of order.
			if (eventDate.before(event.getSynchronizationDate())) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void ack(Event event) {

	}

	@Override
	public Date calculateOutgoingStreamDate() {
		
		synchronized (buffer) {
			// In case the buffer is empty, we can return the default value for the heart beat stream.
			if (this.buffer.size() <= 0) {
				return getCurrentStreamTime();
			}

			// In case we have something in the buffer, but we have nothing processed so far, we return a date long a go.
			if (lastDateProcessed == null) {
				return new Date(0);
			}
			
			return lastDateProcessed;
		}
	}

	@Override
	public void updateIncomingStreamDate(Date streamDate) {
		executeEventsInBuffer();
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
		
		// Since currently all subclasses uses anyway the end date, we can code here this directly.
		return event.getEndDate();
	}


	public Date getLastDateProcessed() {
		synchronized (buffer) {
			return lastDateProcessed;
		}
	}

	public void setLastDateProcessed(Date lastDateProcessed) {
		synchronized (buffer) {
			this.lastDateProcessed = lastDateProcessed;
		}
	}

}
