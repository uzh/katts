package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.uzh.ddis.katts.query.processor.join.JoinConditionConfiguration;
import ch.uzh.ddis.katts.query.processor.join.JoinVariableConfiguration;
import ch.uzh.ddis.katts.query.processor.join.RegularJoinConditionConfiguration;

import com.google.common.collect.HashMultimap;

/**
 * The regular join joins two streams on a set of variables. This is the same as the @{link
 * {@link SameValueJoinCondition} join except that the variables can have different names in the joining streams.
 * 
 * 
 * @author Lorenz Fischer
 * 
 */
public class RegularJoinCondition extends AbstractJoinCondition {

	/** The name of the field to join for each stream. */
	private Map<String, String> fieldNames;

	/**
	 * This set contains the identifiers of all streams this condition works on.
	 */
	private Set<String> streamIds;

	/**
	 * This map contains a multimap for each stream this condition works on. Each multimap contains all the bindings
	 * that share the same value on their respective joinField. The key of this map is the shared value itself.
	 */
	private Map<String, HashMultimap<String, SimpleVariableBindings>> joinCache;

	@Override
	public void prepare(JoinConditionConfiguration configuration, Set<String> streamIds) {

		if (!(configuration instanceof RegularJoinConditionConfiguration)) {
			throw new IllegalStateException("An object of type " + configuration.getClass()
					+ " cannot be used to configure an object of type " + this.getClass());
		} else {
			Set<String> testSet;
			RegularJoinConditionConfiguration castConfiguration = (RegularJoinConditionConfiguration) configuration;

			this.streamIds = streamIds;

			/*
			 * create field name map and make sure there is at least one variable definition for all defined stream
			 * identifiers.
			 */
			testSet = new HashSet<String>(streamIds);
			this.fieldNames = new HashMap<String, String>();
			for (JoinVariableConfiguration joinVariableConfiguration : castConfiguration.getJoinVariables()) {
				testSet.remove(joinVariableConfiguration.getStreamId());
				this.fieldNames.put(joinVariableConfiguration.getStreamId(), joinVariableConfiguration.getJoinField());
			}
			if (testSet.size() > 0) {
				throw new IllegalArgumentException("Missing join variable for the following stream(s): "
						+ testSet.toString());
			}

			// create join cache
			this.joinCache = new HashMap<String, HashMultimap<String, SimpleVariableBindings>>();
			for (String streamId : streamIds) { // create a join map per stream
				HashMultimap<String, SimpleVariableBindings> mapForStream = HashMultimap.create();
				this.joinCache.put(streamId, mapForStream);
			}
		}
	}

	@Override
	public Set<SimpleVariableBindings> join(SimpleVariableBindings newBindings, String fromStreamId) {
		Set<SimpleVariableBindings> result = AbstractJoinCondition.emptySet;
		Object fieldValue = newBindings.get(this.fieldNames.get(fromStreamId));

		if (fieldValue != null) {
			List<Set<SimpleVariableBindings>> setsToJoin;

			// put the bindings object into the join cache
			this.joinCache.get(fromStreamId).put(fieldValue.toString(), newBindings);

			// create the list of streamIds we have to check for matching values
			setsToJoin = new ArrayList<Set<SimpleVariableBindings>>();
			for (String streamId : this.streamIds) {
				if (!streamId.equals(fromStreamId)) { // we only test all other caches
					HashMultimap<String, SimpleVariableBindings> bindingsForValues = this.joinCache.get(streamId);

					if (!bindingsForValues.containsKey(fieldValue)) {
						// we don't have to check any other cache and can safely abort here.
						setsToJoin.clear();
						break;
					}

					setsToJoin.add(bindingsForValues.get(fieldValue.toString()));
				}
			}

			if (setsToJoin.size() > 0) {
				result = new HashSet<SimpleVariableBindings>();
				List<SimpleVariableBindings> cartesianBindings = createCartesianBindings(setsToJoin,
						AbstractJoinCondition.ignoredBindings);
				for (SimpleVariableBindings bindings : cartesianBindings) {
					result.add(merge(newBindings, bindings, AbstractJoinCondition.ignoredBindings));
				}
			}
		}

		/*
		 * only if we found a match in all streams (except the one the new bindings was received on), we return the
		 * resulting set of new bindings.
		 */
		return result;
	}

	@Override
	public void removeBindingsFromCache(SimpleVariableBindings bindings, String streamId) {
		String joinFieldNameForStream = this.fieldNames.get(streamId);
		this.joinCache.get(streamId).remove(bindings.get(joinFieldNameForStream), bindings);
	}

	@Override
	public void removeBindingsFromCache(SimpleVariableBindings bindings) {
		for (String streamId : this.streamIds) { // create a join map per stream
			removeBindingsFromCache(bindings, streamId);
		}
	}

}
