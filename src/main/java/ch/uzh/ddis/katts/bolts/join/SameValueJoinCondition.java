package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.uzh.ddis.katts.query.processor.join.JoinConditionConfiguration;
import ch.uzh.ddis.katts.query.processor.join.SameValueJoinConditionConfiguration;

import com.google.common.collect.HashMultimap;

/**
 * This condition joins the variable bindings of each stream if they share the same value on a given field.
 * 
 * @author Lorenz Fischer
 * 
 */
public class SameValueJoinCondition extends AbstractJoinCondition {

	/**
	 * This set contains the identifiers of all streams this condition works on.
	 */
	private Set<String> streamIds;

	/** The name of the field, this condition joins on. */
	private String joinField;

	/**
	 * This map contains a multimap for each stream this condition works on. Each multimap contains all the bindings
	 * that share the same value on the field with name joinField. The key of this map is the shared value itself.
	 */
	private Map<String, HashMultimap<String, SimpleVariableBindings>> joinCache;

	@Override
	public void prepare(JoinConditionConfiguration configuration, Set<String> streamIds) {
		SameValueJoinConditionConfiguration castConfiguration;

		if (!(configuration instanceof SameValueJoinConditionConfiguration)) {
			throw new IllegalStateException("An object of type " + configuration.getClass()
					+ " cannot be used to configure an object of type " + this.getClass());
		}

		castConfiguration = (SameValueJoinConditionConfiguration) configuration;

		if (castConfiguration.getJoinField() == null) {
			throw new IllegalArgumentException("Missing join field 'joinOn' in configuration: " + configuration);
		}

		this.joinField = castConfiguration.getJoinField();
		this.joinCache = new HashMap<String, HashMultimap<String, SimpleVariableBindings>>();
		this.streamIds = streamIds;

		for (String streamId : this.streamIds) { // create a join map per stream
			HashMultimap<String, SimpleVariableBindings> mapForStream = HashMultimap.create();
			this.joinCache.put(streamId, mapForStream);
		}
	}

	@Override
	public Set<SimpleVariableBindings> join(SimpleVariableBindings newBindings, String fromStreamId) {
		Set<SimpleVariableBindings> result = AbstractJoinCondition.emptySet;
		Object fieldValue = newBindings.get(this.joinField.toString());

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
		this.joinCache.get(streamId).remove(bindings.get(this.joinField), bindings);
	}

	@Override
	public void removeBindingsFromCache(SimpleVariableBindings bindings) {
		for (String streamId : this.streamIds) { // create a join map per stream
			removeBindingsFromCache(bindings, streamId);
		}
	}

}
