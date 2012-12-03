package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.List;

import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.ConsumerConfiguration;
import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * The partitioner configuration provides a interface for the configuration options of the partitioner.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface PartitionerConfiguration extends ProducerConfiguration, ConsumerConfiguration {

	/**
	 * The {@link Variable} on which the partition (group) is build on. This is like the "group by" in SQL languages.
	 * 
	 * @return
	 */
	public Variable getPartitionOn();

	/**
	 * The {@link Variable} on which the aggregation should be processed. This is like the field in the aggregation
	 * functions in SQL languages.
	 * 
	 * @return
	 */
	public Variable getAggregateOn();

	/**
	 * The window size of the sliding window.
	 * 
	 * @return
	 */
	public Duration getWindowSize();

	/**
	 * The slide size of each slide inside a window.
	 * 
	 * @return
	 */
	public Duration getSlideSize();

	/**
	 * This method returns a list of components that builds aggregates on the defined partition.
	 * 
	 * @return
	 */
	public List<PartitionerComponent> getComponents();

}
