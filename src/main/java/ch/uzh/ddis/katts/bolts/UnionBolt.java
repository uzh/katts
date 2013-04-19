/**
 * 
 */
package ch.uzh.ddis.katts.bolts;

import java.util.Date;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.UnionConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This bolts sends the messages from all incoming streams to all outgoing streams creating the "union" of all streams
 * in doing so.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class UnionBolt extends AbstractSynchronizedBolt {

	/** The configuration object holding all the */
	private final UnionConfiguration configuration;

	/**
	 * Creates a new instance of this bolt type using the configuration provided.
	 * 
	 * @param configuration
	 *            the jaxb configuration object.
	 */
	public UnionBolt(UnionConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
	}

	@Override
	public void execute(Event event) {
		SimpleVariableBindings bindings = new SimpleVariableBindings(event.getTuple());

		// emit the variables in bindings on each stream
		for (Stream stream : this.getStreams()) {
			VariableBindings bindingsToEmit = getEmitter().createVariableBindings(stream, event);

			/*
			 * Copy all values from the bindings object into the outgoing binding. This includes the inherited
			 * variables.
			 */
			for (Variable variable : stream.getAllVariables()) {
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
