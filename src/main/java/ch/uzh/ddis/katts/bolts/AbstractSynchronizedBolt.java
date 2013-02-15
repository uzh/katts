package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.HashMap;
import java.util.PriorityQueue;

import backtype.storm.tuple.Tuple;

/**
 * This abstract bolt implementation provides synchronization of incoming events to the subclasses. The implementation
 * employs the heartbeat and the Storm context knowledge for synchronization. This class maintains a buffer with all
 * events that should be synchronized. Hence this implementation enforce a inner state, that should be transferred in
 * case of a rebalancing.
 * 
 * The synchronization is achieved by listening on all task that may send data to this task. If its clear that for a
 * certain event no previous event can occur the event is forwarded to the subclass.
 * 
 * @author Thomas Hunziker
 * 
 */
public abstract class AbstractSynchronizedBolt extends AbstractVariableBindingsBolt {

	private static final long serialVersionUID = 1L;

	// TODO: Move this storage to the central storage facilties.
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

	/**
	 * This method finds all events that can be processed regard to the processing state of previous tasks in the
	 * buffer.
	 */
	private void executeEventsInBuffer() {

		synchronized (buffer) {
			boolean stop = false;
			while (!stop) {
				StreamSynchronizedEventWrapper next = buffer.peek();
				if (next != null && isEventInTemporalOrder(next)) {
					executeSynchronizedEvent(next);
					buffer.remove(next);
				} else {
					stop = true;
				}
			}
		}

	}

	/**
	 * This method determines if a given event is the possible next event to forward to the subclass. The determination
	 * is done the events in the buffer and the heartbeats, which indicates the processing state of previous bolts.
	 * 
	 * @param event
	 * @return
	 */
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
	public Date getProcessingDate() {

		executeEventsInBuffer();

		synchronized (buffer) {
			// In case the buffer is empty, we can return the default value for the heart beat stream.
			if (this.buffer.size() <= 0) {
				return getLowestCurrentHeartBeat();
			}

			// In case we have something in the buffer, but we have nothing processed so far, we return a date long a
			// go.
			if (lastDateProcessed == null) {
				return new Date(0);
			}

			return lastDateProcessed;
		}
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

	/**
	 * This method sets the processing state of this bolt. Subclasses may override this method to update the processing
	 * state depending on their task. The processing state is expressed as the date, which guarantees that no event is
	 * emitted in the subclass before the here set date. This date is used for the heartbeat to communicate the
	 * processing state to subsequent bolts.
	 * 
	 * @param lastDateProcessed
	 *            The last date possible date, which guarantees that no event is emitted before this date.
	 */
	public void setLastDateProcessed(Date lastDateProcessed) {
		synchronized (buffer) {
			this.lastDateProcessed = lastDateProcessed;
		}
	}

}
