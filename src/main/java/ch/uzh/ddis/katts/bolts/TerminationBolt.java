package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.Map;

import ch.uzh.ddis.katts.monitoring.TerminationMonitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

public class TerminationBolt extends AbstractBolt {

	private static final long serialVersionUID = 1L;
	
	private TerminationMonitor monitor;
	private Date lastProcessedDate = new Date();
	
	@Override
	public void executeHeartBeatFreeTuple(Tuple input) {
		// We do not register this bolt for any input stream except the heart beat stream.
	}

	@Override
	public String getId() {
		return "termination_bolt";
	}

	@Override
	public synchronized void updateIncomingStreamDate(Date streamDate) {
		super.updateIncomingStreamDate(streamDate);
		
		if (streamDate.after(new Date())) {
			monitor.terminate(lastProcessedDate);
		}
		else {
			lastProcessedDate = new Date();
		}
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		monitor = TerminationMonitor.getInstance(stormConf);
	}
	
}
