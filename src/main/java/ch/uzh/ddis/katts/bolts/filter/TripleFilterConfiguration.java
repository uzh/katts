package ch.uzh.ddis.katts.bolts.filter;

import java.util.List;

import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.query.processor.filter.TripleCondition;
import ch.uzh.ddis.katts.query.stream.Stream;

public interface TripleFilterConfiguration extends ProducerConfiguration{
	public String getApplyOnSource();
	public String getGroupOn();
	public List<TripleCondition> getConditions();
	public String getId();
	
}
