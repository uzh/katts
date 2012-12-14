package ch.uzh.ddis.katts.bolts;

import java.util.Date;

import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.query.stream.VariableList;

/**
 * An event is a wrapper around the Tuple from Storm. It provides some convenient functionalities for handling incoming
 * events.
 * 
 * @author Thomas Hunziker
 * 
 */
public class Event implements Comparable<Event> {

	private Tuple tuple;
	private Bolt bolt;
	private StreamConsumer emittedOn;
	private long sequenceNumber;
	private Date startDate;
	private Date endDate;

	/**
	 * Constructor without any argument.
	 */
	public Event() {
	}

	/**
	 * Constructor to construct an event from a tuple, a source bolt and an the stream from which this event is coming
	 * from.
	 * 
	 * @param tuple
	 *            The tuple to wrap.
	 * @param bolt
	 *            The bolt on which this event was arriving.
	 * @param emittedOn
	 *            The stream on which this event is coming in.
	 */
	public Event(Tuple tuple, Bolt bolt, StreamConsumer emittedOn) {
		this.setTuple(tuple);
		this.setBolt(bolt);
		this.setEmittedOn(emittedOn);
		this.setSequenceNumber(tuple.getLongByField("sequenceNumber"));
		this.setStartDate((Date) tuple.getValueByField("startDate"));
		this.setEndDate((Date) tuple.getValueByField("endDate"));
	}

	/**
	 * Construct the event from another event. (Copy constructor)
	 * 
	 * @param event
	 */
	public Event(Event event) {
		this.setTuple(event.getTuple());
		this.setBolt(event.getBolt());
		this.setEmittedOn(event.getEmittedOn());
		this.setSequenceNumber(tuple.getLongByField("sequenceNumber"));
		this.setStartDate((Date) tuple.getValueByField("startDate"));
		this.setEndDate((Date) tuple.getValueByField("endDate"));
	}

	/**
	 * This method acknowledge the receiving of a tuple. This is required by storm to provide reliability. Hence this
	 * method is currently not used, because KATTS provides no reliability.
	 * 
	 */
	public void ack() {
		this.getBolt().ack(this);
	}

	@SuppressWarnings("unchecked")
	public <T> T getVariableValue(Variable variable) {
		T returnValue = null;
		Object object = tuple.getValueByField(variable.getName());

		try {
			returnValue = (T) object;
		} catch (Exception e) {
			throw new IllegalStateException("Variable '" + variable.getName() + "' should be of type "
					+ variable.getType().getName());
		}

		return returnValue;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	/**
	 * @return the consumer instance on which this event has been emitted on.
	 */
	public StreamConsumer getEmittedOn() {
		return emittedOn;
	}

	public void setEmittedOn(StreamConsumer emittedOn) {
		this.emittedOn = emittedOn;
	}

	public Bolt getBolt() {
		return bolt;
	}

	public void setBolt(Bolt bolt) {
		this.bolt = bolt;
	}

	@Override
	public int compareTo(Event event) {
		if (getStartDate().after(event.getStartDate())) {
			return -1;
		} else if (getStartDate().before(event.getStartDate())) {
			return 1;
		} else {
			return 0;
		}
	}

	public VariableList getVariables() {
		return this.getEmittedOn().getStream().getAllVariables();
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(long sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public String toString() {

		StringBuilder builder = new StringBuilder();

		builder.append("Event: \n Start:").append(this.getStartDate()).append("\n End: ").append(this.getEndDate())
				.append("\nVariables:");
		for (Variable variable : this.getVariables()) {
			builder.append(" ").append(variable.getName()).append(": ").append(this.getVariableValue(variable))
					.append("\n");
		}

		return builder.toString();

	}

}
