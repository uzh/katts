package ch.uzh.ddis.katts.bolts;

import java.util.Date;

import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.query.stream.VariableList;

public class StreamSynchronizedEventWrapper extends Event {

	private Event event;
	
	private Date synchronizationDate;
	
	public StreamSynchronizedEventWrapper(Event event, Date synchronizationDate) {
		this.event = event;
		this.setSynchronizationDate(synchronizationDate);
	}
	
	public void ack() {
		this.getBolt().ack(this);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getVariableValue(Variable variable) {
		return this.event.getVariableValue(variable);
	}

	public Tuple getTuple() {
		return this.event.getTuple();
	}

	public void setTuple(Tuple tuple) {
		this.event.setTuple(tuple);
	}

	/**
	 * @return the consumer instance on which this event has been emitted on.
	 */
	public StreamConsumer getEmittedOn() {
		return this.event.getEmittedOn();
	}

	public void setEmittedOn(StreamConsumer emittedOn) {
		this.event.setEmittedOn(emittedOn);
	}

	public Bolt getBolt() {
		return this.event.getBolt();
	}

	public void setBolt(Bolt bolt) {
		this.event.setBolt(bolt);
	}

	@Override
	public int compareTo(Event event) {
		Date dateCompareWith = event.getStartDate();
		if (event instanceof StreamSynchronizedEventWrapper) {
			dateCompareWith = ((StreamSynchronizedEventWrapper)event).getSynchronizationDate();
		}
		if (getSynchronizationDate().after((dateCompareWith))) {
			return -1;
		}
		else if (getSynchronizationDate().before(dateCompareWith)) {
			return 1;
		}
		else {
			return 0;
		}
	}
	
	public VariableList getVariables() {
		return this.event.getVariables();
	}

	public long getSequenceNumber() {
		return this.event.getSequenceNumber();
	}

	public void setSequenceNumber(long sequenceNumber) {
		this.event.setSequenceNumber(sequenceNumber);
	}

	public Date getStartDate() {
		return this.event.getStartDate();
	}

	public void setStartDate(Date startDate) {
		this.event.setStartDate(startDate);
	}

	public Date getEndDate() {
		return this.event.getEndDate();
	}

	public void setEndDate(Date endDate) {
		this.event.setEndDate(endDate);
	}

	public Date getSynchronizationDate() {
		return synchronizationDate;
	}

	public void setSynchronizationDate(Date synchronizationDate) {
		this.synchronizationDate = synchronizationDate;
	}

}
