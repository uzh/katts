package ch.uzh.ddis.katts.bolts;

import java.util.Date;

import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

/**
 * This class is a data object that holds all informations about a heartbeat. It is a convenient access to the
 * underlying data.
 * 
 * @see HeartBeatSpout
 * 
 * @author Thomas Hunziker
 * 
 */
public class HeartBeat {

	private Tuple tuple;

	private Date streamDate;

	public HeartBeat(Tuple tuple) {
		this.setTuple(tuple);
		streamDate = (Date) this.getTuple().getValueByField(HeartBeatSpout.STREAM_TIME_FIELD);
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
