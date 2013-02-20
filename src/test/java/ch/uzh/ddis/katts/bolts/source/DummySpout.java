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
	
	private Fields fields;

	public DummySpout(Fields fields) {
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
		declarer.declare(fields);
		// declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this.getConfiguration().getId()),
		// HeartBeatSpout.getHeartBeatFields());
	}

}
