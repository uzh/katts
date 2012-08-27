package ch.uzh.ddis.katts.bolts;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.query.stream.VariableList;

import backtype.storm.tuple.Tuple;

public class Event implements Comparable<Event>{
	
	private Tuple tuple;
	private Bolt bolt;
	private StreamConsumer emittedOn;
	private long sequenceNumber;
	private Date startDate;
	private Date endDate;
	
	public Event(Tuple tuple, Bolt bolt, StreamConsumer emittedOn) {
		this.setTuple(tuple);
		this.setBolt(bolt);
		this.setEmittedOn(emittedOn);
		this.setSequenceNumber(tuple.getLongByField("sequenceNumber"));
		this.setStartDate((Date)tuple.getValueByField("startDate"));
		this.setEndDate((Date)tuple.getValueByField("endDate"));
	}
	
	public void ack() {
		this.getBolt().ack(this);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getVariableValue(Variable variable) {
		T returnValue = null;
		Object object = tuple.get(variable.getName());
		
		try {
			returnValue = (T) object;
		}
		catch(Exception e) {
			throw new IllegalStateException("Variable '" + variable.getName() +"' should be of type " + variable.getType().getName());
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
		}
		else if (getStartDate().before(event.getStartDate())) {
			return 1;
		}
		else {
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

}
