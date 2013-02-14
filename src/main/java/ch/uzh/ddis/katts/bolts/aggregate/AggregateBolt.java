/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregateConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregatorConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

// TODO: make sure data gets output even when only the heartbeat is advanced

/**
 * This is the implementation of the AggregateBolt as documented in {@link AggregateConfiguration}.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class AggregateBolt extends AbstractSynchronizedBolt {

	/** The object containing all confguration details of this bolt. */
	private AggregateConfiguration configuration;

	/** The aggregator manager takes care of all the aggregates we compute. */
	private AggregatorManager aggregatorManager;

	/**
	 * This list contains all field names over which we need to group
	 */
	private ImmutableList<String> groupByFieldNames;

	/**
	 * This key will be used to get the default list of aggregators from teh aggregators map in the case that no
	 * group-by attribute has been specified in the configuration.
	 */
	private final ImmutableList<Object> nonGroupedAggegateKey = ImmutableList.of();

	public AggregateBolt(AggregateConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		AggregatorConfiguration<?>[] configs;
		configs = new AggregatorConfiguration[configuration.getAggregators().size()];

		this.aggregatorManager = new AggregatorManager(this.configuration.getWindowSize(),
				this.configuration.getOutputInterval(), new AggregatorManager.Callback() {

					@Override
					public void callback(Table<ImmutableList<Object>, String, Object> aggregateValues, Date startDate,
							Date endDate) {

						for (ImmutableList<Object> groupingKeyValues : aggregateValues.rowKeySet()) {
							for (String aggregateName : aggregateValues.columnKeySet()) {
								// create the simple variable bindings object where we copy the values from
								SimpleVariableBindings bindings = new SimpleVariableBindings();

								bindings.put("startDate", startDate);
								bindings.put("endDate", endDate);

								// create the values for the group-by fields
								for (int i = 0; i < AggregateBolt.this.groupByFieldNames.size(); i++) {
									bindings.put(AggregateBolt.this.groupByFieldNames.get(i), groupingKeyValues.get(i));
								}

								// now add teh actual aggregate value.. finally!
								bindings.put(aggregateName, aggregateValues.get(groupingKeyValues, aggregateName));

								// we loop over each stream and create the bindings object to emit
								for (Stream stream : getStreams()) {
									// TODO: remove that anchor event stuff. for now using null
									VariableBindings bindingsToEmit = getEmitter().createVariableBindings(stream, null);

									/*
									 * Copy all values from the bindings object into the outgoing binding. This includes
									 * the inherited variables.
									 */
									for (Variable variable : stream.getAllVariables()) {
										if (bindings.get(variable.getReferencesTo()) == null) {
											throw new NullPointerException("Could not find a value for variable \""
													+ variable.getReferencesTo() + "\" in the bindings object.");
										}
										bindingsToEmit.add(variable, bindings.get(variable.getReferencesTo()));
									}
									bindingsToEmit.setStartDate((Date) bindings.get("startDate"));
									bindingsToEmit.setEndDate((Date) bindings.get("endDate"));

									bindingsToEmit.emit();
								}

							}
						}

						// set the date that will be propagated by the heartbeat
						setLastDateProcessed(endDate);
					}
				}, this.configuration.isOnlyIfChanged(), this.configuration.getAggregators().toArray(configs));

		if (this.configuration.getGroupBy() != null) {
			this.groupByFieldNames = ImmutableList.copyOf(this.configuration.getGroupBy().split(","));
		}
	}

	@Override
	public void execute(Event event) {
		SimpleVariableBindings bindings = new SimpleVariableBindings(event.getTuple());
		ImmutableList<Object> groupByKey;

		if (groupByFieldNames == null) { // no grouping configured
			groupByKey = this.nonGroupedAggegateKey;
		} else {
			// create the group-by key
			ImmutableList.Builder<Object> builder = ImmutableList.builder();
			for (Object fieldName : this.groupByFieldNames) {
				builder.add(bindings.get(fieldName));
			}
			groupByKey = builder.build();
		}

		this.aggregatorManager.incorporateValue(bindings.getEndDate().longValue(), groupByKey, bindings);
	}

	@Override
	public String getId() {
		return this.configuration.getId();
	}

}
