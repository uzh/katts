package ch.uzh.ddis.katts.monitoring;

/**
 * This interface defines the functionality for a termination watcher. A termination watcher can register himself in the
 * TerminationMonitor to be informed, when the query is terminated.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface TerminationWatcher {

	public void terminated();

}
