package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import ch.uzh.ddis.katts.query.processor.join.EvictionRuleConfiguration;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;

/**
 * Eviction rules define conditions under which certain elements can be evicted from the caches of {@link JoinCondition}
 * and the eviction rule indices. They make use of optimized data structures in order to keep the set of variable
 * bindings in the current join cache relevant.
 * 
 * @author fischer
 */
public class EvictionRuleManager {

	public enum DT {
		START {
			@Override
			public Long getTime(SimpleVariableBindings bindings) {
				return ((Date) bindings.get("startDate")).getTime();
			}
		},
		END {
			public Long getTime(SimpleVariableBindings bindings) {
				return ((Date) bindings.get("endDate")).getTime();
			}
		};

		public abstract Long getTime(SimpleVariableBindings bindings);
	}

	/** A set containing all ids of all incoming streams. */
	private Set<String> streamIds;

	/** The join condition this bolt uses. */
	private JoinCondition joinCondition;

	/**
	 * Rules for the {@link #executeBeforeEvictionRules(SimpleVariableBindings, String)} method grouped by the
	 * identifier of the stream on which they have to operate. See
	 * {@link #executeEvitionRules(List, SimpleVariableBindings, String)} for more information.
	 * 
	 * @see EvictionRuleManager#executeAfterEvictionRules(SimpleVariableBindings, String)
	 */
	private HashMultimap<String, EvictionRuleConfiguration> beforeEvictionRules;

	/**
	 * The for the {@link #executeAfterEvictionRules(SimpleVariableBindings, String)} method will execute. See
	 * {@link #executeEvitionRules(List, SimpleVariableBindings, String)} for more information.
	 * 
	 * @see EvictionRuleManager#executeAfterEvictionRules(SimpleVariableBindings, String)
	 */
	private HashMultimap<String, EvictionRuleConfiguration> afterEvictionRules;

	/**
	 * For each <b>stream</b> we keep a sorted {@link TreeMap} of all variable bindings for both the <b>start</b> and
	 * the <b>end</b> time of the variable bindings that arrived over it. This data structure keeps all this information
	 * as follows:
	 * 
	 * <pre>
	 * Map&lt;streamId, Map&lt;DT, TreeMap&lt;TimeValue, HashSet&lt;VariableBinding&gt;&gt;&gt;&gt;
	 * </pre>
	 * 
	 * <ul>
	 * <li>The key of the outer most map is the identifier of the stream.</li>
	 * <li>The key of the map inside the first map states if the contained treemap is sorted by either the start
	 * (DT.START) or end (DT.END) time of the variable binding</li>
	 * <li>The key of the TreeMap is a long value representing the standard java time of the variable binding.</li>
	 * <li>The inner most HashSet then contains all variable bindings for the given date, date type, and stream.</li>
	 * </ul>
	 */
	private HashMap<String, HashMap<DT, TreeMap<Long, HashSet<SimpleVariableBindings>>>> evictionCaches;

	/**
	 * After creating the manager, new bindings can be added to its indexes using the
	 * {@link #addBindingsToIndices(SimpleVariableBindings, String)} method. The methods
	 * {@link #executeBeforeEvictionRules(SimpleVariableBindings, String)} and
	 * {@link #executeAfterEvictionRules(SimpleVariableBindings, String)} methods will evict variable bindings from the
	 * indexes of this manager and also from the index of the provided {@link #joinCondition}.
	 * 
	 * @param beforeEvictionRules
	 *            a collection of configuration details for the eviction rules that have to be executed <i>before</i>
	 *            the actual join takes place.
	 * @param afterEvictionRules
	 *            a collection of configuration details for the eviction rules that have to be executed <i>before</i>
	 *            the actual join takes place.
	 * @param joinCondition
	 *            the configured join condition instance. This reference is necessary, so the rules in this manager can
	 *            remove the evicted variable bindings from the join cache accordingly.
	 * @param streamIds
	 *            a set containing the identifiers of all streams this condition will process data of.
	 * 
	 * @see #addBindingsToIndices(SimpleVariableBindings, String)
	 * @see #executeBeforeEvictionRules(SimpleVariableBindings, String)
	 * @see #executeAfterEvictionRules(SimpleVariableBindings, String)
	 */
	public EvictionRuleManager(List<EvictionRuleConfiguration> beforeEvictionRules,
			List<EvictionRuleConfiguration> afterEvictionRules, JoinCondition joinCondition, Set<String> streamIds) {
		this.joinCondition = joinCondition;
		this.streamIds = streamIds;

		/*
		 * create a sorted treemap for both the start times and the end times of variable bindings per stream
		 */
		this.evictionCaches = new HashMap<String, HashMap<DT, TreeMap<Long, HashSet<SimpleVariableBindings>>>>();
		for (String streamId : streamIds) {
			HashMap<DT, TreeMap<Long, HashSet<SimpleVariableBindings>>> dtMap = new HashMap<DT, TreeMap<Long, HashSet<SimpleVariableBindings>>>();

			for (DT dt : DT.values()) {
				dtMap.put(dt, new TreeMap<Long, HashSet<SimpleVariableBindings>>());
			}

			this.evictionCaches.put(streamId, dtMap);
		}

		// Create the multi maps for both the before and the after rules.
		this.beforeEvictionRules = createRuleMaps(beforeEvictionRules);
		this.afterEvictionRules = createRuleMaps(afterEvictionRules);
	}

	/**
	 * This method creates the rule index maps for fast rule retrieval. The are later used by the
	 * {@link #executeEvitionRules(List, SimpleVariableBindings, String)} method.
	 * 
	 * @see EvictionRuleManager#executeEvitionRules(List, SimpleVariableBindings, String)
	 * @return a map based on the
	 */
	private HashMultimap<String, EvictionRuleConfiguration> createRuleMaps(List<EvictionRuleConfiguration> rules) {
		HashMultimap<String, EvictionRuleConfiguration> result = HashMultimap.create();

		for (EvictionRuleConfiguration rule : rules) {
			for (String streamId : evaluateIdExpression(rule.getOn())) {
				result.put(streamId, rule);
			}
		}

		return result;
	}

	/**
	 * This method evaluates the configured expression of the 'from' and 'to' fields in the configuration of an eviction
	 * rule. These fields can contain either a single star ("*") character or a comma separated list of stream
	 * identifiers. The validity of the stream identifiers are tested against the contents of the member variable
	 * {@link #streamIds}.
	 * 
	 * @param expression
	 *            the configuration expression to be evaluated.
	 * @return a list containing all identifiers configured by the supplied expression
	 */
	private List<String> evaluateIdExpression(String expression) {
		List<String> result = new ArrayList<String>();

		if (expression == null || expression.length() == 0) {
			throw new IllegalArgumentException("Missing field in eviction rule configuration: " + expression);
		}

		if (expression.equals("*")) {// add the rule for all stream ids
			result.addAll(this.streamIds);
		} else {
			// add all configured stream ids
			Collections.addAll(result, expression.split(","));
		}

		// make sure we have no configuration errors
		for (String configuredStreamId : result) {
			if (!this.streamIds.contains(configuredStreamId)) {
				throw new IllegalArgumentException("Unknown streamId in rule configuration: " + expression
						+ " only the following stream identifiers have been configured: " + this.streamIds);
			}
		}

		return result;
	}

	/**
	 * Calling this method will run all eviction rules that have been configured to be ran "before" the join.
	 * 
	 * <b>Please Note:</b> This method will not add the bindings to this managers indices. You will have to do this
	 * yourself using the {@link #addBindingsToIndices(SimpleVariableBindings, String)} method.
	 * 
	 * @param newBindings
	 *            the new variable bindings object that has been received.
	 * @param arrivedOnStreamId
	 *            the identifier of the stream the variable bindings have been received on.
	 */
	public void executeBeforeEvictionRules(SimpleVariableBindings newBindings, String arrivedOnStreamId) {
		executeEvitionRules(this.beforeEvictionRules, newBindings, arrivedOnStreamId);
	}

	/**
	 * Calling this method will run all eviction rules that have been configured to be ran "after" the join.
	 * 
	 * <b>Please Note:</b> This method will not add the bindings to this managers indices. You will have to do this
	 * yourself using the {@link #addBindingsToIndices(SimpleVariableBindings, String)} method.
	 * 
	 * @param newBindings
	 *            the new variable bindings object that has been received.
	 * @param arrivedOnStreamId
	 *            the identifier of the stream the variable bindings have been received on.
	 */
	public void executeAfterEvictionRules(SimpleVariableBindings newBindings, String arrivedOnStreamId) {
		executeEvitionRules(this.afterEvictionRules, newBindings, arrivedOnStreamId);
	}

	/**
	 * Executes the provided eviction rules for the new provided bindings that have arrived on the stream identified by
	 * arrivedOnStreamId.
	 * 
	 * The user can specify the stream on which the rule is "listening" on (using the "on" field). This field accepts a
	 * comma separated list of stream identifiers or a single star ('*') character. Whenever a new variable binding
	 * arrives, we check which rules need to be executed based on the identifier of the stream the variable binding
	 * arrived on. The way the "from" field es evaluated works analogous. See {@link #createRuleMaps()} and
	 * {@link #evaluateIdExpression(String)} for how the index maps we use here are being created.
	 * 
	 * @param evictionRules
	 *            the rules to execute.
	 * @param newBindings
	 *            the new variable bindings object that has been received.
	 * @param arrivedOnStreamId
	 *            the identifier of the stream the variable bindings have been received on.
	 * @see #createRuleMaps()
	 */
	private void executeEvitionRules(HashMultimap<String, EvictionRuleConfiguration> evictionRules,
			SimpleVariableBindings newBindings, String arrivedOnStreamId) {
		StandardEvaluationContext expressionContext = new StandardEvaluationContext();
		ExpressionParser expressionParser = new SpelExpressionParser();

		/*
		 * Example expression: * #{ from.getLongByField('endDate') < on.getLongByField('startDate') }
		 * 
		 * since we expect most of the eviction expression being based on 'endDate' and 'startDate' our objects support
		 * these two fields in the java bean notation
		 * 
		 * #{ from.endDate < on.startDate }
		 */
		expressionContext.setVariable("on", newBindings);

		// execute rules!
		for (EvictionRuleConfiguration rule : evictionRules.get(arrivedOnStreamId)) {
			Expression expression = expressionParser.parseExpression(rule.getCondition());

			for (String fromStreamId : evaluateIdExpression(rule.getFrom())) {
				String condition = rule.getCondition();
				TreeMap<Long, HashSet<SimpleVariableBindings>> sortedMap;

				// find the most efficient index to use
				if (condition.contains("from.endDate")) {
					sortedMap = this.evictionCaches.get(fromStreamId).get(DT.END);
				} else if (condition.contains("from.startDate")) {
					sortedMap = this.evictionCaches.get(fromStreamId).get(DT.START);
				} else {
					throw new IllegalArgumentException(
							"Missing date in if-condition. You must specify either 'from.startDate' "
									+ "or 'from.endDate' in rule configuration: " + rule);
				}

				// TODO lorenz: support equals conditions!
				/*
				 * Our treemap is sorted by the date of the "from" condition. We start looking for items to evict
				 * starting from both ends of the treemap and stop as soon as the expression is false for the first
				 * time. This way we make sure, that we only process as many items as we absolutely have to.
				 */
				for (NavigableSet<Long> set : ImmutableList.of(sortedMap.navigableKeySet(),
						sortedMap.descendingKeySet())) {
					Iterator<Long> iter = set.iterator();
					List<SimpleVariableBindings> bindingsToRemove = new ArrayList<SimpleVariableBindings>();

					while (iter.hasNext()) {
						Long current = iter.next();
						Iterator<SimpleVariableBindings> setIterator;

						if (!sortedMap.containsKey(current)) {
							// we reached the end of this search run - abort the
							// loop!
							break;
						}

						setIterator = sortedMap.get(current).iterator();
						while (setIterator.hasNext()) {
							SimpleVariableBindings bindings = setIterator.next();
							expressionContext.setVariable("from", bindings);
							if (expression.getValue(expressionContext, Boolean.class)) {
								// hit! remember to remove it after the loop
								bindingsToRemove.add(bindings);
							}
						}
					}

					// now remove the f**kers from all indices and caches
					for (SimpleVariableBindings bindings : bindingsToRemove) {
						this.joinCondition.removeBindingsFromCache(bindings, fromStreamId);
						for (DT dt : DT.values()) {
							TreeMap<Long, HashSet<SimpleVariableBindings>> treeMap;
							Set<SimpleVariableBindings> bindingsSet;

							treeMap = this.evictionCaches.get(fromStreamId).get(dt);
							bindingsSet = treeMap.get(dt.getTime(bindings));
							bindingsSet.remove(bindings);
							// remove empty sets from the index
							if (bindingsSet.size() == 0) {
								treeMap.remove(dt.getTime(bindings));
							}
						}
					}
				}
			}
		}
	}

	/**
	 * Adds the given variable bindings to the indices of this manager.
	 * 
	 * @param newBindings
	 *            the new variable bindings object that has been received.
	 * @param arrivedOnStreamId
	 *            the identifier of the stream the variable bindings have been received on.
	 */
	public void addBindingsToIndices(SimpleVariableBindings newBindings, String arrivedOnStreamId) {
		for (DT dt : DT.values()) {
			Long date = dt.getTime(newBindings);
			HashSet<SimpleVariableBindings> bindingsSet = this.evictionCaches.get(arrivedOnStreamId).get(dt).get(date);

			if (bindingsSet == null) {
				bindingsSet = new HashSet<SimpleVariableBindings>();
				this.evictionCaches.get(arrivedOnStreamId).get(dt).put(date, bindingsSet);
			}

			bindingsSet.add(newBindings);
		}
	}
}
