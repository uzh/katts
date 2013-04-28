package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.List;

import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.ConsumerConfiguration;
import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.bolts.SynchronizedConfiguration;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * The partitioner configuration provides a interface for the configuration options of the partitioner.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface PartitionerConfiguration extends ProducerConfiguration, ConsumerConfiguration, SynchronizedConfiguration {

	/**
	 * The {@link Variable} on which the partition (group) is built on. This is like the "group by" in SQL languages.
	 * 
	 * @return The variable on which the partition is built on.
	 */
	public Variable getPartitionOn();

	/**
	 * The {@link Variable} on which the aggregation should be processed. This is like the field in the aggregation
	 * functions in SQL languages.
	 * 
	 * @return The variable on which the values are aggregated.
	 */
	public Variable getAggregateOn();

	/**
	 * The window size of the sliding window. For the aggregation a window is moving over the data over the time.
	 * 
	 * @return The window size of the sliding window.
	 */
	public Duration getWindowSize();

	/**
	 * The slide size of each slide inside a window.
	 * 
	 * @return The size of the slide of the sliding window.
	 */
	public Duration getSlideSize();

	/**
	 * This method returns a list of components that builds aggregates on the defined partition.
	 * 
	 * @return
	 */
	public List<PartitionerComponent> getComponents();

}
