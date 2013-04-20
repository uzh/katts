package ch.uzh.ddis.katts.monitoring;

import java.util.Map;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;

/**
 * This class implements a monitor per task to record the messages sending behavior.
 * 
 * @author Thomas Hunziker
 * 
 */
public class TaskMonitor extends BaseTaskHook {

	private Recorder recorder;
	private int thisTaskId;
	
	/**
	 * We use this variable to selectively record messages. If it is true, the {@link #emit(EmitInfo)} method
	 * will record the messages of this task, otherwise messages will be ignored.
	 */
	private boolean recordMessages;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {

		String topologyId = context.getStormId();

		this.thisTaskId = context.getThisTaskId();
		this.recorder = Recorder.getInstance(stormConf, topologyId);

		this.recordMessages = true;
//		// only record messages that are not from a "source" component
//		this.recordMessages = !context.getThisComponentId().toLowerCase().contains("source");
		
	}

	@Override
	public void emit(EmitInfo info) {
		if (this.recordMessages && !info.stream.contains("__ack")) { // don't measure ack messages
			for (Integer taskId : info.outTasks) {
				this.recorder.recordMessageSending(this.thisTaskId, taskId);
			}
		}
	}

}
