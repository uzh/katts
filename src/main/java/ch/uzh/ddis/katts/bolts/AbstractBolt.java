package ch.uzh.ddis.katts.bolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

/**
 * This Bolt implements the basics for a Bolt, that requires a handling for heart beats.
 * 
 * 
 * @author Thomas Hunziker
 * 
 */
public abstract class AbstractBolt implements IRichBolt {

	private OutputCollector collector;

	private Map<Integer, HeartBeat> lastHeartBeatPerTask = new HashMap<Integer, HeartBeat>();
	private List<Integer> tasksFromIncomingStreams = new ArrayList<Integer>();
	private Date currentStreamTime;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		// Find all tasks from incoming streams
		Map<GlobalStreamId, Grouping> sources = context.getThisSources();
		for (Entry<GlobalStreamId, Grouping> entry : sources.entrySet()) {
			List<Integer> tasks = context.getComponentTasks(entry.getKey().get_componentId());
			for (Integer taskId : tasks) {
				tasksFromIncomingStreams.add(taskId);
			}
		}

	}

	@Override
	public final void execute(Tuple input) {
		
//		this.ack(input);

		boolean isHeartBeatTuple = true;

		// This synchronization is required, because we should not allow the execution of a heart beat tuple in parallel
		// with a regular tuple. The reason is that, when the regular tuple processing takes more time than the
		// processing of the heart beat tuple, we may emit a wrong stream processing time.
		synchronized (this) {
			String heartBeatStreamId = HeartBeatSpout.buildHeartBeatStreamId(input.getSourceComponent());

			if (heartBeatStreamId.equals(input.getSourceStreamId())) {
				HeartBeat heartBeat = new HeartBeat(input);
				executeHeartBeat(heartBeat);
			} else {
				isHeartBeatTuple = false;
			}
		}

		if (!isHeartBeatTuple) {
			executeHeartBeatFreeTuple(input);
		}
	}

	public abstract void executeHeartBeatFreeTuple(Tuple input);

	public abstract String getId();

	/**
	 * This method handles the heart beat tuples. This method should not be overridden. Override instead the methods
	 * {@link AbstractBolt#calculateOutgoingStreamDate()} or {@link AbstractBolt#updateIncomingStreamDate()}.
	 * 
	 * @param heartBeat
	 */
	public synchronized void executeHeartBeat(HeartBeat heartBeat) {

		// We synchronize the different heart beats from the different tasks. We will emit the heart beat with the
		// lowest stream real time. We emit the heart beat only when the heart beat comes from the first task in the
		// set.

		// Add heart beat to our list.
		lastHeartBeatPerTask.put(heartBeat.getTaskId(), heartBeat);

		// Check if we have the first heart beat got:
		Integer firstTaskInList = tasksFromIncomingStreams.iterator().next();
		if (firstTaskInList.equals(heartBeat.getTaskId())) {

			HeartBeat lowestHeartBeat = null;

			// Find the heart beat with the lowest stream time
			for (Entry<Integer, HeartBeat> entry : lastHeartBeatPerTask.entrySet()) {
				if (lowestHeartBeat == null || entry.getValue().getStreamDate().before(lowestHeartBeat.getStreamDate())) {
					lowestHeartBeat = entry.getValue();
				}
			}

			currentStreamTime = lowestHeartBeat.getStreamDate();
			updateIncomingStreamDate(lowestHeartBeat.getStreamDate());

			this.collector.emit(HeartBeatSpout.buildHeartBeatStreamId(this.getId()),
					HeartBeatSpout.getOutputTuple(lowestHeartBeat.getTuple(), calculateOutgoingStreamDate()));
			
			
//			System.out.println(String.format("Componet %1s with Heart Beat at: %2s", this.getId(), currentStreamTime.toString()));

		}

	}

	public synchronized Date calculateOutgoingStreamDate() {
		return getCurrentStreamTime();
	}

	public synchronized void updateIncomingStreamDate(Date streamDate) {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Define the output fields for the heart beat
		declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this.getId()), HeartBeatSpout.getHeartBeatFields());
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {

	}

	public synchronized final Date getCurrentStreamTime() {
		return currentStreamTime;
	}
	
	public List<Integer> getAllSourceTasksFromIncomingStreams() {
		return tasksFromIncomingStreams;
	}
	
	public synchronized HeartBeat getLastHeartBeatPerTask(Integer taskId) {
		return this.lastHeartBeatPerTask.get(taskId);
	}
	
	public synchronized void emit(String streamId, Tuple anchor, List<Object> tuple) {
		// We do not emit the anchor, to prevent the tracking of the tuples
		this.collector.emit(streamId, tuple);
	}
	
	private synchronized void ack(Tuple tuple) {
		this.collector.ack(tuple);
	}
	
}
