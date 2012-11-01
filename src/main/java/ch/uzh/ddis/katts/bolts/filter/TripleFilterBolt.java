package ch.uzh.ddis.katts.bolts.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.query.processor.filter.TripleCondition;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.utils.XmlTypeMapping;

/**
 * This class is used to filter out certain triples. This is the first step for
 * every stream consumed by the katts system. The output of this filter are
 * variable bindings. All other components work then only with these variable
 * bindings.
 * 
 * Since this bolt does not consumes variable bindings, it does not implement
 * the {@ch.uzh.ddis.katts.bolts.Bolt} interface.
 * 
 * @author Thomas Hunziker
 * 
 */
public class TripleFilterBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private TripleFilterConfiguration configuration;
	private long counter = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		setCollector(collector);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tuple) {
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
						value = XmlTypeMapping.converFromString(
								tuple.getStringByField(reference),
								variable.getType());
						output.add(value);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				getCollector().emit(stream.getId(), output);
				counter++;
			}
		}

		getCollector().ack(tuple);
	}

	private boolean tupleMatchConditions(Tuple tuple) {
		// Apply Filter
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

	public OutputCollector getCollector() {
		return collector;
	}

	public void setCollector(OutputCollector collector) {
		this.collector = collector;
	}

	public List<Stream> getStreams() {
		return configuration.getProducers();
	}

	public void setConfiguration(TripleFilterConfiguration configuration) {
		this.configuration = configuration;
	}

}