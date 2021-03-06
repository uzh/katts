/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.xml.datatype.Duration;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.SumConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

import com.google.common.collect.ImmutableList;

/**
 * This bolts computes the sum of an integer field over a specified time window. It supports grouping over one or
 * multiple fields and supports a configurable output interval.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SumBolt extends AbstractSynchronizedBolt {

	/** The configuration object holding all the */
	private final SumConfiguration configuration;

	/**
	 * This map holds all the SumCreator instances that we use to compute the sum values. For each combination of
	 * "group-by" fields, we keep a separate sum creator instance.
	 */
	private Map<ImmutableList<Object>, SumCreator> sumCreators;

	/** If no group by fields have been specified, we only create one sum creator and store it in this reference. */
	private SumCreator nonGroupedSumCreator;

	/** The name of the field, the value of which is to be summed over. */
	private String sumFieldName;

	/** The name under which the sum can be referenced in the outgoing stream configuration. */
	private String referenceName;

	/**
	 * This list contains all field names over which we need to group
	 */
	private ImmutableList<String> groupByFieldNames;

	/**
	 * Creates a new instance of this bolt type using the configuration provided.
	 * 
	 * @param configuration
	 *            the jaxb configuration object.
	 */
	public SumBolt(SumConfiguration configuration) {
		super(configuration.getBufferTimeout(), configuration.getWaitTimeout());
		this.configuration = configuration;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		// initialize the data structures
		if (this.configuration.getGroupBy() == null || this.configuration.getGroupBy().length() == 0) {
			this.nonGroupedSumCreator = SumCreator.createSumCreator(this.configuration.getWindowSize());
		} else {
			/*
			 * Create a "default map". Whenever we don't find a creator for a key, we create a new sum creator and store
			 * it in the map
			 */
			this.sumCreators = new HashMap<ImmutableList<Object>, SumCreator>() {
				private Duration duration = configuration.getWindowSize();

				@SuppressWarnings("unchecked")
				@Override
				public SumCreator get(Object key) {
					SumCreator creator = super.get(key);

					if (creator == null) {
						creator = SumCreator.createSumCreator(duration);
						super.put((ImmutableList<Object>) key, creator);
					}

					return creator;
				}
			};
		}

		if (this.configuration.getField() == null) {
			throw new IllegalStateException("Missing 'field=\"field_name\"' in the configuration.");
		} else if (this.configuration.getAs() == null) {
			throw new IllegalStateException("Missing 'as=\"ref_name\"' in the configuration.");
		}

		this.sumFieldName = this.configuration.getField();
		this.referenceName = this.configuration.getAs();

		if (this.configuration.getGroupBy() != null) {
			this.groupByFieldNames = ImmutableList.copyOf(this.configuration.getGroupBy().split(","));
		}
	}

	@Override
	public void execute(Event event) {
		SimpleVariableBindings bindings = new SimpleVariableBindings(event.getTuple());
		double value;
		double sum;
		SumCreator sumCreator;

		if (this.nonGroupedSumCreator != null) { // no grouping configured
			sumCreator = this.nonGroupedSumCreator;
		} else {
			// find the sum creator for the current group-by configuration
			ImmutableList<Object> groupByKey;
			ImmutableList.Builder<Object> builder = ImmutableList.builder();

			for (String fieldName : this.groupByFieldNames) {
				builder.add(bindings.get(fieldName));
			}

			groupByKey = builder.build();
			sumCreator = this.sumCreators.get(groupByKey);
		}

		value = ((Double) bindings.get(this.sumFieldName)).doubleValue();
		sum = sumCreator.add(event.getEndDate().getTime(), value);
		// store the sum into our variable bindings data structure using the name
		bindings.put(this.referenceName, sum);

		// emit the new sum value results by creating a variableBindings object for each stream
		for (Stream stream : this.getStreams()) {
			VariableBindings bindingsToEmit = getEmitter().createVariableBindings(stream, event);

			/*
			 * Copy all values from the bindings object into the outgoing binding. This includes the inherited
			 * variables.
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

		ack(event);
	}

	@Override
	public String getId() {
		return this.configuration.getId();
	}

}
