package ch.uzh.ddis.katts.bolts;

import java.util.Collection;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.IRichBolt;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

public interface Bolt extends IRichBolt{
	public Collection<StreamConsumer> getStreamConsumer();

	public void setConsumerStreams(Collection<StreamConsumer> streamConsumer);

	public Collection<Stream> getStreams();

	public void setStreams(Collection<Stream> streams);

	public OutputCollector getCollector();

	public void execute(Event event);
	
	public void ack(Event event);
}
