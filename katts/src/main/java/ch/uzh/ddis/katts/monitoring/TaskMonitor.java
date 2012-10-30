package ch.uzh.ddis.katts.monitoring;

import java.util.Map;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;

public class TaskMonitor extends BaseTaskHook {
	
	private Recorder recorder;
	private int thisTaskId;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
		String topologyId = context.getStormId();
		
		thisTaskId = context.getThisTaskId();
		recorder = Recorder.getInstance(stormConf, topologyId);
		
	}

	@Override
	public void emit(EmitInfo info) {
		for (Integer taskId : info.outTasks) {
			recorder.recordMessageSending(thisTaskId, taskId);
		}
	}
	

}
