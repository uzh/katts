package ch.uzh.ddis.katts.bolts.filter;

import java.util.List;

import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.query.processor.filter.NTupleCondition;

/**
 * This interface provides the configuration options for the triple filter. 
 * 
 * @author Thomas Hunziker
 *
 */
public interface NTupleFilterConfiguration extends ProducerConfiguration {
	
	/**
	 * The apply on source indicates on which source the triple filter is applied on. 
	 * 
	 * @return The source component id on which the triple filter is applied on.
	 */
	public String getApplyOnSource();
	
	/**
	 * The list of conditions that an n-tuple must meet to be processed further. 
	 * 
	 * @return The list of conditions that the n-tuple must meet to be processed further.
	 */
	public List<NTupleCondition> getConditions();
	
}
