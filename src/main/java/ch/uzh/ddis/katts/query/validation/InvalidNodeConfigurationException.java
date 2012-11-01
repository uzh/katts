package ch.uzh.ddis.katts.query.validation;

/**
 * This exception is thrown if a node configuration is not valid.
 * 
 * @author Thomas Hunziker
 *
 */
public class InvalidNodeConfigurationException extends Exception{

	private static final long serialVersionUID = 1L;
	
	public InvalidNodeConfigurationException(String message) {
		super(message);
	}
}
