package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * This class implements an abstract implementation of the {@link JoinCondition} with convenient methods for processing
 * join conditions.
 * 
 * @author Lorenz Fischer
 * 
 */
public abstract class AbstractJoinCondition implements JoinCondition {

	/**
	 * When merging variable binding in the {@link #merge(SimpleVariableBindings, SimpleVariableBindings)} method we
	 * ignore all keys that appear in this list.
	 */
	protected static final Set<String> ignoredBindings = ImmutableSet.of("sequenceNumber");

	/**
	 * This empty set will be returned by the join method if the new binding could not be joined the the ones already in
	 * the cache.
	 */
	protected static final ImmutableSet<SimpleVariableBindings> emptySet = ImmutableSet.of();

	/**
	 * Merges b1 and b2.
	 * 
	 * @param b1
	 *            the first bindings object to join.
	 * @param b2
	 *            the second bindings object to join
	 * @param ignoredBindings
	 *            in case of a value conflict between between the two sets of bindings (i.e. two values for the same
	 *            variable) this method is going to throw an exception except the conflicting variable is contained in
	 *            this set.
	 * @return a new variable bindings object containing the contents of both.
	 */
	protected SimpleVariableBindings merge(SimpleVariableBindings b1, SimpleVariableBindings b2,
			Set<String> ignoredBindings) {
		SimpleVariableBindings mergedBindings = new SimpleVariableBindings();
		mergedBindings.putAll(b1);

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
					} else if (ignoredBindings == null || !ignoredBindings.contains(key)) {
						throw new IllegalStateException(String.format("Conflicting values for key '%1s' "
								+ "when trying to merge variable bindings %2s into %s3.", key, b2.toString(),
								mergedBindings.toString()));
					}

					mergedBindings.put(key, newValue);
				}
			} else {
				mergedBindings.put(key, b2.get(key));
			}
		}

		return mergedBindings;
	}

	/**
	 * Recursively merges the list of sets variable bindings so that the result is the cartesian product of all sets.
	 * 
	 * @param setsToJoin
	 * @param ignoredBindings
	 *            in case of a value conflict between between the two sets of bindings (i.e. two values for the same
	 *            variable) this method is going to throw an exception except the conflicting variable is contained in
	 *            this set.
	 * @return a set containing the joined results
	 */
	protected List<SimpleVariableBindings> createCartesianBindings(List<Set<SimpleVariableBindings>> setsToJoin,
			Set<String> ignoredBindings) {
		List<SimpleVariableBindings> result;

		if (setsToJoin.size() == 1) {
			result = new ArrayList<SimpleVariableBindings>(setsToJoin.get(0));
		} else {
			result = new ArrayList<SimpleVariableBindings>();

			for (SimpleVariableBindings outer : setsToJoin.get(0)) {
				for (SimpleVariableBindings inner : createCartesianBindings(setsToJoin.subList(1, setsToJoin.size()),
						ignoredBindings)) {
					result.add(merge(outer, inner, ignoredBindings));
				}
			}
		}

		return result;
	}

}
