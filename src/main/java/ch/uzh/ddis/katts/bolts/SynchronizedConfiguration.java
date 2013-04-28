package ch.uzh.ddis.katts.bolts;

import ch.uzh.ddis.katts.query.processor.AbstractSynchronizedProcessor;

/**
 * Synchronized processors need to have a bufferTimeout and a waitTimeout configuration option.
 * 
 * @author Lorenz Fischer
 * 
 * @see AbstractSynchronizedProcessor
 */
public interface SynchronizedConfiguration extends Configuration {

	/**
	 * @see AbstractSynchronizedProcessor
	 * @return the buffer timeout
	 */
	public long getBufferTimeout();

	/**
	 * @see AbstractSynchronizedProcessor
	 * @return the wait timeout
	 */
	public long getWaitTimeout();

}
