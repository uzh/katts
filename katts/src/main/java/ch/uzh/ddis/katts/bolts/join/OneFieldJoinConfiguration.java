package ch.uzh.ddis.katts.bolts.join;

import ch.uzh.ddis.katts.bolts.Configuration;
import ch.uzh.ddis.katts.query.stream.Variable;

public interface OneFieldJoinConfiguration extends Configuration{
	
	/**
	 * This method returns the maximal number of variable bindings
	 * in one queue. For each stream that is join and for each
	 * join variable one queue is maintained. This indicates 
	 * the maximal size of one such queue. 
	 * 
	 * @return
	 */
	public int getMaxBufferSize();
	
	
	/**
	 * This {@link Variable} indicates on which values the join
	 * is done. The variable is used to find out which value
	 * is used.
	 * 
	 * @return
	 */
	public Variable getJoinOn();
	
	/**
	 * This method returns the precision for a join. The precision
	 * is given in milliseconds that the time can differ between the joining
	 * events. 
	 * 
	 * @return
	 */
	public long getJoinPrecision();
	
}
