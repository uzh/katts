package ch.uzh.ddis.katts.bolts;

import java.util.Collection;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

public interface Bolt extends IRichBolt{
	public Collection<StreamConsumer> getStreamConsumer();

	public void setConsumerStreams(Collection<StreamConsumer> streamConsumer);

	/**
	 * @return a collection containing all the outgoing streams of this bolt. 
	 */
	public Collection<Stream> getStreams();

	public void setStreams(Collection<Stream> streams);

	public void emit(String streamId, Tuple anchor, List<Object> tuple);

	public void execute(Event event);
	
	public void ack(Event event);
	
//	public void ack(Tuple tuple);
}
