package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
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

	@Override
	public void executeRegularTuple(Tuple input) {
		// We do not register this bolt for any input stream except the heart beat stream. Hence we do not process
		// anything.
	}

	@Override
	public String getId() {
		return "termination_bolt";
	}

	@Override
	public synchronized Date getOutgoingStreamDate(Date streamDate) {
		
		if (streamDate.after(new Date())) {
			monitor.terminate(lastProcessedDate);
		} else {
			lastProcessedDate = new Date();
		}

		logger.info(String.format("Current Termination bolt heart beat time: %1s", streamDate.toString()));
		
		return streamDate;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		monitor = TerminationMonitor.getInstance(stormConf);
	}

}
