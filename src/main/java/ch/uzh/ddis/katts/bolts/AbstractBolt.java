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
 * This Bolt implements the basics for a Bolt. This are:
 * 
 * <ul>
 * <li>Heart Beat Handling: Each Bolt, even if he has no state, must handle the heartbeat. This abstract implementation
 * provides convenient methods to make the handling easier. Normally for stateless Bolts no additional handling must be
 * added. Statefull Bolts must ensure that they update the heartbeat date, with the current processing state.</li>
 * <li>Data Collection: The emit infrastructure is not thread safe. This abstract bolt provides synchronization on the
 * collector.</li>
 * </ul>
 * 
 * This bolt has for heartbeat some storage facilities. But this data is no crucial and hence can be lost at any point
 * in time. Hence this implementation can threaded as stateless bolt.
 * 
 * @author Thomas Hunziker
 * 
 */
public abstract class AbstractBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	private Map<Integer, HeartBeat> lastHeartBeatPerTask = new HashMap<Integer, HeartBeat>();
	private List<Integer> tasksFromIncomingStreams = new ArrayList<Integer>();
	private Date currentStreamTime;

	private Boolean heartBeatMonitor = new Boolean(true);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
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

		// This synchronization is required, because we should not allow the execution of a heart beat tuple in parallel
		// with a regular tuple. The reason is that, when the regular tuple processing takes more time than the
		// processing of the heart beat tuple, we may emit a wrong stream processing time.
		synchronized (heartBeatMonitor) {
			String heartBeatStreamId = HeartBeatSpout.buildHeartBeatStreamId(input.getSourceComponent());

			if (heartBeatStreamId.equals(input.getSourceStreamId())) {
				HeartBeat heartBeat = new HeartBeat(input);
				executeHeartBeat(heartBeat);
			} else {
				executeRegularTuple(input);
			}
		}
	}

	/**
	 * This method is invoked, when a regular (non-heartbeat) tuple is executed. The input Tuple contains all the
	 * information provided by Storm for this data item.
	 * 
	 * @param input
	 *            The input tuple with all variables and the source task information.
	 */
	public abstract void executeRegularTuple(Tuple input);

	/**
	 * This method returns the components id.
	 * 
	 * @return The component id in the storm topology.
	 */
	public abstract String getId();

	/**
	 * This method handles the heart beat tuples. This method should not be overridden. Override instead the methods
	 * {@link AbstractBolt#getOutgoingStreamDate()} or {@link AbstractBolt#updateIncomingStreamDate()}.
	 * 
	 * @param heartBeat
	 */
	protected void executeHeartBeat(HeartBeat heartBeat) {

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

			this.emit(
					HeartBeatSpout.buildHeartBeatStreamId(this.getId()),
					HeartBeatSpout.getOutputTuple(lowestHeartBeat.getTuple(),
							getOutgoingStreamDate(lowestHeartBeat.getStreamDate())));
		}

	}

	/**
	 * This method is used to retrieve the processing state of subclasses. The heartbeat must communicate the processing
	 * state to subsequent bolts. The processing state is the date of the last date, that can guarantees that no earlier
	 * date is emitted by the sub bolt.
	 * 
	 * @return The processing state for the heartbeat of this bolt.
	 */
	public Date getOutgoingStreamDate(Date streamDate) {
		return streamDate;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Define the output fields for the heart beat
		declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this.getId()), HeartBeatSpout.getHeartBeatFields());
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {

	}

	/**
	 * This method returns the current processing state of all previous bolts. It is the lowest date, of all current
	 * heartbeats over all streams.
	 * 
	 * @return The processing state of previous bolts.
	 */
	public final Date getCurrentStreamTime() {
		synchronized (heartBeatMonitor) {
			return currentStreamTime;
		}
	}

	/**
	 * This method returns all tasks that may send events to this bolt.
	 * 
	 * @return
	 */
	public List<Integer> getAllSourceTasksFromIncomingStreams() {
		return tasksFromIncomingStreams;
	}

	/**
	 * This method returns the heartbeat of a certain task id.
	 * 
	 * @param taskId
	 *            The id of the task for which the heartbeat shoul be returned.
	 * @return null or the heartbeat indicated by the taskId.
	 */
	public HeartBeat getLastHeartBeatPerTask(Integer taskId) {
		synchronized (heartBeatMonitor) {
			return this.lastHeartBeatPerTask.get(taskId);
		}
	}

	/**
	 * This method emits a tuple on the indicated streamId. This method is thread safe.
	 * 
	 * @param streamId
	 *            The stream id on which the tuple should be emitted.
	 * @param anchor
	 *            The tuple at which the tuple should be bound. Currently this is ignored, because KATTS provides anyway
	 *            no reliability and hence this would produce unnecessary overhead.
	 * @param tuple
	 *            The tuple to emit.
	 */
	public void emit(String streamId, Tuple anchor, List<Object> tuple) {
		// We do not emit the anchor, to prevent the tracking of the tuples
		this.emit(streamId, tuple);
	}

	/**
	 * This method emits a tuple on the indicated streamId. This method is thread safe.
	 * 
	 * @param streamId
	 *            The stream id on which the tuple should be emitted.
	 * @param tuple
	 *            The tuple to emit.
	 */
	public void emit(String streamId, List<Object> tuple) {
		synchronized (this.collector) {
			this.collector.emit(streamId, tuple);
		}
	}

	/**
	 * This method acknowledge the receiving of a tuple. This is required by storm to provide reliability. Hence this
	 * method is currently not used, because KATTS provides no reliability.
	 * 
	 * @param tuple
	 */
	public synchronized void ack(Tuple tuple) {
		synchronized (this.collector) {
			this.collector.ack(tuple);
		}
	}

}
