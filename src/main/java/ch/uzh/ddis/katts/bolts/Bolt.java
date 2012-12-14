package ch.uzh.ddis.katts.bolts;

import java.util.Collection;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * This interface extends the Storm {@link IRichBolt} by providing additional functionalities. Such as list of outgoing
 * streams, a list of incoming streams, an emitting method and so on.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface Bolt extends IRichBolt {

	/**
	 * This method returns a collection of consuming streams. A Bolt consumes always one or more stream. This collection
	 * returns a list of them.
	 * 
	 * @return
	 */
	public Collection<StreamConsumer> getStreamConsumer();

	/**
	 * Sets the consumers of this bolt.
	 * 
	 * @param streamConsumer
	 */
	public void setConsumerStreams(Collection<StreamConsumer> streamConsumer);

	/**
	 * @return a collection containing all the outgoing streams of this bolt.
	 */
	public Collection<Stream> getStreams();

	/**
	 * Set the collection of producing streams by this bolt.
	 * 
	 * @param streams
	 */
	public void setStreams(Collection<Stream> streams);

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
	public void emit(String streamId, Tuple anchor, List<Object> tuple);

	/**
	 * This method is invoked when ever an event is arriving and the class should process the event.
	 * 
	 * @param event
	 *            The event to process.
	 */
	public void execute(Event event);

	
	/**
	 * This method acknowledge the receiving of a tuple. This is required by storm to provide reliability. Hence this
	 * method is currently not used, because KATTS provides no reliability.
	 * 
	 * @param tuple
	 */
	public void ack(Event event);
}
