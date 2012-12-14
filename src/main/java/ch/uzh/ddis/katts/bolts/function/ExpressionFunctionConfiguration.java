package ch.uzh.ddis.katts.bolts.function;

import ch.uzh.ddis.katts.bolts.ProducerConfiguration;

/**
 * The expression function configuration is used to provide an interface for the configuration options required by the
 * {@link ExpressionFunctionBolt}.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface ExpressionFunctionConfiguration extends ProducerConfiguration{

	/**
	 * This method returns the expression, which should be evaluated.
	 * 
	 * @return The expression that must be meet that a certain variable binding is processed further.
	 */
	public String getExpression();

}
