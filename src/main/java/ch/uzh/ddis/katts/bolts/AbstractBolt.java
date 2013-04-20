package ch.uzh.ddis.katts.bolts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;

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

	private List<Integer> tasksFromIncomingStreams = new ArrayList<Integer>();

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
		executeRegularTuple(input);
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
		this.collector.emit(streamId, anchor, tuple);
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
