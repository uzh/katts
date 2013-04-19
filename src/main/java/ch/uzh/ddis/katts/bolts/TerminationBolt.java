package ch.uzh.ddis.katts.bolts;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;

/**
 * For the detection of the query termination the heartbeat is monitored. This bolt does the monitoring.
 * 
 * At the end of each input file a triple in the future is added. This triple produces a heartbeat in the future. When
 * this heartbeat reaches the last processing bolt, the query termination is terminated. This termination bolt is
 * attached to the last processing node and listen on the heartbeat. When the termination is detected, a ZooKeeper entry
 * is written and the other components of the system can react on this.
 * 
 * @author Thomas Hunziker
 * 
 */
public class TerminationBolt extends AbstractBolt {

	private static final long serialVersionUID = 1L;

	private TerminationMonitor monitor;
	private Date lastProcessedDate = new Date();
	private Logger logger = LoggerFactory.getLogger(TerminationBolt.class);

	private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public void executeRegularTuple(Tuple input) {
		if (input.getLongByField("EndDate") > System.currentTimeMillis()) {
			logger.info("WE ARE DONE!");
		}
	}

	@Override
	public String getId() {
		return "termination_bolt";
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// Sir, nothing to declare, Sir!
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		monitor = TerminationMonitor.getInstance(stormConf);
	}

}
