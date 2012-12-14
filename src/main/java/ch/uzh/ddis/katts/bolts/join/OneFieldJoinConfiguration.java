package ch.uzh.ddis.katts.bolts.join;

import ch.uzh.ddis.katts.bolts.Configuration;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This interface defines the configurations required by the {@link OneFieldJoinBolt}.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface OneFieldJoinConfiguration extends Configuration {

	/**
	 * This {@link Variable} indicates on which values the join is done. The variable is used to find out which value is
	 * used.
	 * 
	 * @return The variable to join on.
	 */
	public Variable getJoinOn();

	/**
	 * This method returns the precision for a join. The precision is given in milliseconds that the time can differ
	 * between the joining events.
	 * 
	 * @return The temporal precision of the join.
	 */
	public long getJoinPrecision();

}
