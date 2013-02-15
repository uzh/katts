package ch.uzh.ddis.katts.bolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public abstract class AbstractBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	private Map<Integer, HeartBeat> lastHeartBeatPerTask = new ConcurrentHashMap<Integer, HeartBeat>();
	private List<Integer> tasksFromIncomingStreams = new ArrayList<Integer>();

	/**
	 * The lowest current hearbeat is the the oldest date value among all the heartbeat messages that have been received
	 * by this bolt.
	 */
	private Date lowestCurrentHeartBeat;

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

		/*
		 * This synchronization is required, because we should not allow the execution of a heart beat tuple in parallel
		 * with a regular tuple. The reason is that, when the regular tuple processing takes more time than the
		 * processing of the heart beat tuple, we may emit a wrong stream processing time.
		 * 
		 * TODO: remove this hack. I'm not even sure this works, because we cannot make any assumptions on how fast a
		 * tuple is transmitted over the network. better solution: update the "lastTupleDateWeSent" only when the tuple
		 * has been 'acked'.
		 */
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
		lastHeartBeatPerTask.put(heartBeat.getSourceTaskId(), heartBeat);

		/*
		 * In order to prevent duplication of heartbeat messages, we only update and emit the heartbeat state of this
		 * node, when we receive a heartbeat message from one task. Here, we use the first task in the list of all the
		 * tasks that are sending heartbeats. However, This could be any task, really.
		 */
		if (tasksFromIncomingStreams.get(0).equals(heartBeat.getSourceTaskId())) {

			HeartBeat lowestHeartBeat = null;

			// Find the heart beat with the lowest stream time
			for (Entry<Integer, HeartBeat> entry : lastHeartBeatPerTask.entrySet()) {
				if (lowestHeartBeat == null || entry.getValue().getStreamDate().before(lowestHeartBeat.getStreamDate())) {
					lowestHeartBeat = entry.getValue();
				}
			}

			setLowestCurrentHeartBeat(lowestHeartBeat.getStreamDate());

			this.emit(HeartBeatSpout.buildHeartBeatStreamId(this.getId()),
					HeartBeatSpout.getOutputTuple(lowestHeartBeat.getTuple(), getProcessingDate()));
		}

	}

	/**
	 * This method sets a new value for the lowest current heartbeat. Each bolt keeps track of all the current (or last
	 * received) heartbeat dates for all the incoming tasks it is connected to. The lowest current hearbeat is the the
	 * the oldest date value from all of these.
	 * <p/>
	 * This method is called at the heartbeat rate.
	 * 
	 * @param value
	 *            the new date to use as the lowestCurrentHeartBeat date.
	 */
	protected void setLowestCurrentHeartBeat(Date value) {
		this.lowestCurrentHeartBeat = value;
	}

	/**
	 * Each bolt keeps track of all the current (or last received) heartbeat dates for all the incoming tasks it is
	 * connected to. The lowest current hearbeat is the the the oldest date value from all of these.
	 * 
	 * @return the lowest value among all the heartbeat dates.
	 */
	public final Date getLowestCurrentHeartBeat() {
		return this.lowestCurrentHeartBeat;
	}

	/**
	 * <p/>
	 * This method is used to retrieve the processing date of this bolt. The processing date is the date up until which
	 * we can assume all messages have been processed by this bolt. This could either be the endDate of the message we
	 * emitted last (for synchronized bolts) or some other date that. Whatever date we return in this function, we need
	 * to guarantee that this bolt is never going to emit a tuple with a date that is older than this processing date.
	 * 
	 * <p/>
	 * The default implementation returns the value of {@link #getLowestCurrentHeartBeat()} and in doing so just passes
	 * on the processing date of the "slowest" preceeding task. This default implementation will most likely be used by
	 * all non-synchronized bolts. These are mostly stateless bolts in which the temporal order of tuples don't matter.
	 * Since we cannot reasonably make any guarantees about what tuples have been processed already, we just pass on the
	 * value form the preceeding bolt.
	 * 
	 * @return the processing date of this bolt.
	 */
	public Date getProcessingDate() {
		return getLowestCurrentHeartBeat();
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
		return this.lastHeartBeatPerTask.get(taskId);
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
