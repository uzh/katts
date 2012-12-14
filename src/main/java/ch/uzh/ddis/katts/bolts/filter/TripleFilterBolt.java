package ch.uzh.ddis.katts.bolts.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.bolts.AbstractBolt;
import ch.uzh.ddis.katts.query.processor.filter.TripleCondition;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.utils.XmlTypeMapping;

/**
 * This class is used to filter out certain triples. This is the first step for every stream consumed by the katts
 * system. The output of this filter are variable bindings. All other components work then only with these variable
 * bindings.
 * 
 * Since this bolt does not consumes variable bindings, it does not implement the {@link ch.uzh.ddis.katts.bolts.Bolt}
 * interface. However it uses the Heartbeat infrastructure hence it inherit from the {@link AbstractBolt}.
 * 
 * @author Thomas Hunziker
 * 
 */
public class TripleFilterBolt extends AbstractBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private TripleFilterConfiguration configuration;
	private long counter = 0;

	@Override
	public void executeRegularTuple(Tuple tuple) {

		if (tupleMatchConditions(tuple)) {
			for (Stream stream : configuration.getProducers()) {
				List<Object> output = new ArrayList<Object>();

				// First three variables are for the synchronization and
				// ordering of the events
				output.add(counter);
				output.add(tuple.getValueByField("startDate"));
				output.add(tuple.getValueByField("endDate"));

				for (Variable variable : stream.getAllVariables()) {
					String reference = variable.getReferencesTo();
					Object value;
					try {
						value = XmlTypeMapping.converFromString(tuple.getStringByField(reference), variable.getType());
						output.add(value);
					} catch (Exception e) {
						throw new RuntimeException("Cant convert the given input value to the desired output data type.", e);
					}
				}

				this.emit(stream.getId(), tuple, output);
				counter++;
			}
		}

		this.ack(tuple);
	}

	/**
	 * This method checks if a incoming tuple matches the defined condition. If so the method returns true.
	 * 
	 * @param tuple
	 * @return True, if the tuple meets the conditions defined in the configuration.
	 */
	private boolean tupleMatchConditions(Tuple tuple) {
		// Apply Filter by iterating over the conditions and matches them on the tuple values.
		for (TripleCondition condition : this.configuration.getConditions()) {
			String value = tuple.getStringByField(condition.getItem());
			if (!value.equals(condition.getRestriction())) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		super.declareOutputFields(declarer);

		for (Stream stream : this.getStreams()) {
			List<String> fields = new ArrayList<String>();
			fields.add("sequenceNumber");
			fields.add("startDate");
			fields.add("endDate");
			for (Variable variable : stream.getAllVariables()) {
				fields.add(variable.getName());
			}

			declarer.declareStream(stream.getId(), new Fields(fields));
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public List<Stream> getStreams() {
		return configuration.getProducers();
	}

	public void setConfiguration(TripleFilterConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public String getId() {
		return this.configuration.getId();
	}

}
