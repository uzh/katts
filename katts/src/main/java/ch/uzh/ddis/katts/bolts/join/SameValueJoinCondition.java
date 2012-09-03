package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.uzh.ddis.katts.query.processor.join.JoinConditionConfiguration;
import ch.uzh.ddis.katts.query.processor.join.SameValueJoinConditionConfiguration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;

/**
 * This condition joins the variable bindings of each stream if they share the same value on a given field.
 * 
 * @author fischer
 * 
 */
public class SameValueJoinCondition implements JoinCondition {

	/**
	 * When merging variable binding in the {@link #merge(SimpleVariableBindings, SimpleVariableBindings)} method we
	 * ignore all keys that appear in this list.
	 */
	private static final Set<String> ignoredBindings = ImmutableSet.of("sequenceNumber");

	/**
	 * This empty set will be returned by the join method if the new binding could not be joined the the ones already in
	 * the cache.
	 */
	private static final ImmutableSet<SimpleVariableBindings> emptySet = ImmutableSet.of();

	/**
	 * This set contains the identifiers of all streams this condition works on.
	 */
	private Set<String> streamIds;

	/** The name of the field, this condition joins on. */
	private String joinField;

	/**
	 * This map contains a multimap for each stream this condition works on. Each multimap contains all the bindings
	 * that share the same value on the field with name joinField.
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
		Set<SimpleVariableBindings> result = emptySet;
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
						break;
					}

					setsToJoin.add(bindingsForValues.get(fieldValue.toString()));
				}
			}

			if (setsToJoin.size() > 0) {
				result = new HashSet<SimpleVariableBindings>();
				for (SimpleVariableBindings bindings : createCartesianBindings(setsToJoin)) {
					result.add(merge(newBindings, bindings));
				}
			}
		}

		/*
		 * only if we found a match in all streams (except the one the new bindings was received on), we return the
		 * resulting set of new bindings.
		 */
		return result;
	}

	/**
	 * Merges b1 and b2.
	 * 
	 * @param b1
	 *            the first bindings object to join.
	 * @param b2
	 *            the second bindings object to join
	 * @return a new variable bindings object containing the contents of both.
	 */
	private SimpleVariableBindings merge(SimpleVariableBindings b1, SimpleVariableBindings b2) {
		SimpleVariableBindings mergedBindings = new SimpleVariableBindings();

		for (String key : b1.keySet()) {
			if (!ignoredBindings.contains(key)) {
				mergedBindings.put(key, b1.get(key));
			}
		}

		/*
		 * For now, we hard code the handling of startDate and endDate conflicts here. For the future, we could think of
		 * creating a conflict management facility.
		 */
		for (String key : b2.keySet()) {
			if (mergedBindings.containsKey(key)) {
				if (!mergedBindings.get(key).equals(b2.get(key))) {
					Object existingValue = mergedBindings.get(key);
					Object conflictingValue = b2.get(key);
					Object newValue = existingValue;

					if (key.equals("startDate")) {
						Date d1 = (Date) existingValue;
						Date d2 = (Date) conflictingValue;
						newValue = new Date(Math.min(d1.getTime(), d2.getTime()));
					} else if (key.equals("endDate")) {
						Date d1 = (Date) existingValue;
						Date d2 = (Date) conflictingValue;
						newValue = new Date(Math.max(d1.getTime(), d2.getTime()));
					} else {
						throw new IllegalStateException(String.format("Conflicting values for key '%1s' "
								+ "when trying to merge variable bindings %2s into %s3.", key, b2.toString(),
								mergedBindings.toString()));
					}

					mergedBindings.put(key, newValue);
				} else {
					mergedBindings.put(key, b2.get(key));
				}
			}
		}

		return mergedBindings;
	}

	/**
	 * Recursively merges the list of sets variable bindings so that the result is the cartesian product of all sets.
	 * 
	 * @param setsToJoin
	 * @return a set containing the joined results
	 */
	private List<SimpleVariableBindings> createCartesianBindings(List<Set<SimpleVariableBindings>> setsToJoin) {
		List<SimpleVariableBindings> result;

		if (setsToJoin.size() == 1) {
			result = new ArrayList<SimpleVariableBindings>(setsToJoin.get(0));
		} else {
			result = new ArrayList<SimpleVariableBindings>();

			for (SimpleVariableBindings outer : setsToJoin.get(0)) {
				for (SimpleVariableBindings inner : createCartesianBindings(setsToJoin.subList(1, setsToJoin.size()))) {
					result.add(merge(outer, inner));
				}
			}
		}

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
