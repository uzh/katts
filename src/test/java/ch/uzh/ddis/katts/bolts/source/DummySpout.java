/**
 * 
 */
package ch.uzh.ddis.katts.bolts.source;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.CompletableSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

/**
 * This dummy Spout is used in the Test cases instead of the regular FileReaderBolts.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class DummySpout extends BaseRichSpout {

	public static final Object EXHAUSTED_MARK = "End Of Mock Input";
	
	private String streamName;
	private Fields fields;

	/**
	 * Creates a new dummy spout with one stream (named streamname) having the fields <code>fields</code>.
	 * 
	 * This class supports only one outoing stream at the moment.
	 * 
	 * @param streamName
	 * @param fields
	 */
	public DummySpout(String streamName, Fields fields) {
		this.streamName = streamName;
		this.fields = fields;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
	}

	@Override
	public void nextTuple() {
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(this.streamName, fields);
		// declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this.getConfiguration().getId()),
		// HeartBeatSpout.getHeartBeatFields());
	}

}
