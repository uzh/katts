package ch.uzh.ddis.katts.query.processor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;

/**
 * Synchonized processors will be implemented by a sublcass of {@link AbstractSynchronizedBolt}. For this reason this
 * configuration object provides configuration parameters for the bufferTimeout and the waitTimeout.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class AbstractSynchronizedProcessor extends AbstractProcessor {

	/**
	 * Events will be kept in the buffer at least for this many milliseconds, before processing them in temporal order.
	 * This allows for small irregularities in the ordering within each incoming stream.
	 */
	@XmlAttribute
	private long bufferTimeout = 1000;

	/**
	 * After this many milliseconds, an incoming stream will be assumed to have no more elements, so processing goes on.
	 * This is to guard against stalling in case of a filter bolt stopping to send anything.
	 */
	@XmlAttribute
	private long waitTimeout = 2000;

	public long getBufferTimeout() {
		return bufferTimeout;
	}

	public void setBufferTimeout(long bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	public long getWaitTimeout() {
		return waitTimeout;
	}

	public void setWaitTimeout(long waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

}
