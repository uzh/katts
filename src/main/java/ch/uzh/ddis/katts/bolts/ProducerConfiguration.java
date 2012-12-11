package ch.uzh.ddis.katts.bolts;

import java.util.List;

import ch.uzh.ddis.katts.query.stream.Stream;

/**
 * A producer is bolt that emits data on regular data streams. This interface defines the basic configuration for such
 * Bolts resp. Spouts.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface ProducerConfiguration extends Configuration {
	public List<Stream> getProducers();
}
