package ch.uzh.ddis.katts.bolts.join;

import java.util.Set;

import ch.uzh.ddis.katts.query.processor.join.JoinConditionConfiguration;

/**
 * This is the general contract that all join conditions have to fulfill.
 * 
 * @author Lorenz Fischer
 * 
 */
public interface JoinCondition {

	/**
	 * This method will be called before the actual processing begins and is
	 * meant for the implementing class to setup all the necessary data
	 * structures.
	 * 
	 * @param configuration
	 *            the object containing all the configuration details for the
	 *            condition.
	 * @param streamIds
	 *            a set containing the identifiers of all streams this condition
	 *            will process data of.
	 */
	public void prepare(JoinConditionConfiguration configuration, Set<String> streamIds);

	/**
	 * This method adds a new item to the cache of this condition. The returned
	 * set will contain all joined variable bindings.
	 * 
	 * @param newBindings
	 *            the new binding which is to be matched against the elements
	 *            already in the join cache.
	 * @param streamId
	 *            the identifier of the stream this object arrived on.
	 * @return the set containing all newly joined variable bindings. The return
	 *         value will never be <i>null</i>.
	 */
	public Set<SimpleVariableBindings> join(SimpleVariableBindings newBindings, String streamId);

	/**
	 * This method removes the given bindings object from the cache for a given
	 * streamId. In order to optimize their internal caches, join conditions
	 * might store variable bindings separately for each stream the temporal
	 * bolt works on. For this reason a user might hint in which cache the
	 * binding that has to be removed is stored in. This method will be invoked
	 * by the various eviction rules.
	 * 
	 * @param bindings
	 *            the bindings to remove.
	 * @param streamId
	 *            the identifier of the stream on which the variable bindings
	 *            object to be removed was initially received on.
	 */
	public void removeBindingsFromCache(SimpleVariableBindings bindings, String streamId);

	/**
	 * This method removes the given bindings object from the cache irrespective
	 * of the stream the variable bindings have been received on. This will most
	 * commonly be invoked by the various eviction rules.
	 * 
	 * @param bindings
	 *            the bindings to remove.
	 */
	public void removeBindingsFromCache(SimpleVariableBindings bindings);

}
