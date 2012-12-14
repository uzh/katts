package ch.uzh.ddis.katts.query.processor.join;

import ch.uzh.ddis.katts.bolts.join.JoinCondition;

/**
 * @author Lorenz Fischer
 */
public interface JoinConditionConfiguration {

	/**
	 * @return the class that implements the join as a bolt.
	 */
	public Class<? extends JoinCondition> getImplementingClass();

}
