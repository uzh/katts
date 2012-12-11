package ch.uzh.ddis.katts.spouts.file;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

/**
 * The heartbeat spout sends in a regular interval a heartbeat to all triple sources. 
 * 
 * @author Thomas Hunziker
 *
 */
public class HeartBeatSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;

	private HeartBeatConfiguration configuration;

	/**
	 * This field contains the system time of the heart beat spout.
	 */
	public static final String HEART_BEAT_TIME_FIELD = "heart_beat_time";

	/**
	 * This field contains the logical time of the stream.
	 */
	public static final String STREAM_TIME_FIELD = "stream_time";

	/**
	 * This field contains the system time of the last processing component.
	 */
	public static final String COMPONENT_TIME_FIELD = "component_time";

	private long waitTime = 3000;

	public static final String HEARTBEAT_STREAMID = "heartbeat";

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		if (this.getConfiguration() != null && this.getConfiguration().getHeartBeatInterval() > 0) {
			waitTime = this.getConfiguration().getHeartBeatInterval();
		}
		else {
			// Set the waitTime to 3000 miliseconds, when nothing was defined.
			waitTime = 3000;
		}
		
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public synchronized void nextTuple() {
		collector.emit(HEARTBEAT_STREAMID, getOutputTuple(new Date(), new Date(0), new Date()));
		Utils.sleep(waitTime);
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(HEARTBEAT_STREAMID, getHeartBeatFields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public HeartBeatConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(HeartBeatConfiguration configuration) {
		this.configuration = configuration;
	}

	public static Fields getHeartBeatFields() {
		return new Fields(HEART_BEAT_TIME_FIELD, STREAM_TIME_FIELD, COMPONENT_TIME_FIELD);
	}

	/**
	 * This method generates a heart beat stream id based on the component id.
	 * 
	 * @param componentId
	 * @return
	 */
	public static String buildHeartBeatStreamId(String componentId) {
		return String.format("heartbeat_%1s", componentId);
	}

	public static List<Object> getOutputTuple(Date heartBeatTime, Date streamTime, Date componentTime) {
		List<Object> tuple = new ArrayList<Object>();

		tuple.add(heartBeatTime);
		tuple.add(streamTime);
		tuple.add(componentTime);

		return tuple;
	}

	public static List<Object> getOutputTuple(Tuple heartBeatTuple, Date streamTime) {
		List<Object> tuple = new ArrayList<Object>();

		tuple.add(heartBeatTuple.getValueByField(HEART_BEAT_TIME_FIELD));
		tuple.add(streamTime);
		tuple.add(new Date());

		return tuple;

	}

}
