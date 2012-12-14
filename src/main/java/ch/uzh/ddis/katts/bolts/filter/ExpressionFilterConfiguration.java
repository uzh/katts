package ch.uzh.ddis.katts.bolts.filter;

import ch.uzh.ddis.katts.bolts.Configuration;

/**
 * The ExpressionFilterConfiguration provides a interface for the minimal configuration options for an
 * {@link ExpressionFilterBolt}.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface ExpressionFilterConfiguration extends Configuration {

	/**
	 * This method returns the expression used to filter the stream.
	 * 
	 * @return The filter expression formulated in SpEL.
	 */
	public String getExpression();

}
