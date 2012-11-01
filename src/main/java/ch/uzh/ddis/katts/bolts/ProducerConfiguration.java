package ch.uzh.ddis.katts.bolts;

import java.util.List;

import ch.uzh.ddis.katts.query.stream.Stream;

public interface ProducerConfiguration extends Configuration{
	public List<Stream> getProducers();
}
