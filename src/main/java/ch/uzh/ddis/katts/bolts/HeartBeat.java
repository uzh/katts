package ch.uzh.ddis.katts.bolts;

import java.util.Date;

import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

import backtype.storm.tuple.Tuple;

public class HeartBeat {
	
	private Tuple tuple;
	
	private Date streamDate;
	
	public HeartBeat(Tuple tuple) {
		this.setTuple(tuple);
		streamDate = (Date)this.getTuple().getValueByField(HeartBeatSpout.STREAM_TIME_FIELD);
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}
	
	
	public Integer getTaskId() {
		return tuple.getSourceTask();
	}

	public Date getStreamDate() {
		return streamDate;
	}

}
