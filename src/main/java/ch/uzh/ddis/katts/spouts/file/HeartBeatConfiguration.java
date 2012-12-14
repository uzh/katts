package ch.uzh.ddis.katts.spouts.file;

/**
 * This interface defines the configuration options for the heartbeat.
 * 
 * @see HeartBeatSpout
 * 
 * @author Thomas Hunziker
 *
 */
public interface HeartBeatConfiguration {
	
	public long getHeartBeatInterval();
}
