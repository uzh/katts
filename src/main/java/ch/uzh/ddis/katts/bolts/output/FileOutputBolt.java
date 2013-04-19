package ch.uzh.ddis.katts.bolts.output;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import ch.uzh.ddis.katts.bolts.AbstractVariableBindingsBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This Bolt writes all the input streams into a file. This is convenient way to store the output data of a query.
 * 
 * @author Thomas Hunziker
 * 
 */
public class FileOutputBolt extends AbstractVariableBindingsBolt {

	private static final long serialVersionUID = 1L;
	private FileOutputConfiguration configuration;
	private DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd H:m:s");
	private Logger logger = LoggerFactory.getLogger(FileOutputBolt.class);
	private StreamConsumer stream;
	private CSVWriter writer;
	private int numberOfColumns;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		FileWriter fstream;
		try {
			fstream = new FileWriter(this.getConfiguration().getFilePath());
			writer = new CSVWriter(fstream);
		} catch (IOException e) {
			throw new RuntimeException("Could not open file writer.", e);
		}

		// TODO: Currently the output stream can only consume one
		// stream. This should be changed to multiple streams.
		stream = this.getStreamConsumer().iterator().next();
		numberOfColumns = stream.getStream().getAllVariables().size() + 2;
		String[] headerLine = new String[numberOfColumns];
		headerLine[0] = "Start";
		headerLine[1] = "End";

		int i = 2;
		for (Variable variable : stream.getStream().getAllVariables()) {
			headerLine[i] = variable.getName();
			i++;
		}

		writer.writeNext(headerLine);
	}

	@Override
	public synchronized void execute(Event event) {
		String[] values = new String[numberOfColumns];
		values[0] = formatter.format(event.getStartDate());
		values[1] = formatter.format(event.getEndDate());

		int i = 2;
		for (Variable variable : stream.getStream().getAllVariables()) {
			Object variableValue = event.getVariableValue(variable);
			if (variableValue == null) {
				if (this.configuration.isAllowNullValues()) {
					values[i] = "null";
				} else {
					throw new IllegalStateException(String.format("Missing variable '%1s' in event: %2s",
							variable.toString(), event.toString()));
				}
			} else {
				values[i] = variableValue.toString();
			}

			i++;
		}
		writer.writeNext(values);
		try {
			writer.flush();
		} catch (IOException e) {
			logger.error(String.format("Couldn't write output into file '%1s'", this.getConfiguration().getFilePath()));
		}

		ack(event);
	}

	public FileOutputConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(FileOutputConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public String getId() {
		return this.getConfiguration().getId();
	}

}
