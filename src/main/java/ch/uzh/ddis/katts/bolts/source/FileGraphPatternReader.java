package ch.uzh.ddis.katts.bolts.source;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.query.source.FileGraphPatternReaderConfiguration;
import ch.uzh.ddis.katts.utils.Util;

/**
 * This Spout reads in triples from files. By the abstraction of the {@link Source} different type of files can be read
 * with this spout.
 * 
 * All triples that share the same timestamp will be matched against the configured graph pattern. If there is a match
 * all variables from the graph pattern will be emitted on the configured stream.
 * 
 * Multiple files can be configured. There will be one spout instance for each file.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class FileGraphPatternReader extends AbstractLineReader {

	private FileGraphPatternReaderConfiguration configuration;

	/** We keep track of how many lines we have read using this variable. */
	private int lastLineRead = 0;

	/**
	 * Creates a new reader instance using the provided configuration.
	 * 
	 * @param configuration
	 *            the configuration object.
	 */
	public FileGraphPatternReader(FileGraphPatternReaderConfiguration configuration) {
		super(configuration);
		this.configuration = configuration;
	}

	@Override
	public boolean nextTuple(Source source) {
		boolean result;

		List<String> triple = null;

		try {
			triple = source.getNextTuple();
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read next triple because: %1s", e.getMessage()), e);
		}

		if (triple != null) {
			List<Object> tuple = new ArrayList<Object>();
			Date semanticDate;
			final String dateStringValue;

			// parse the date field, this supports raw millisecond values and ISO formatted datetime strings
			dateStringValue = triple.get(0);
			if (dateStringValue.contains("-") || dateStringValue.contains("T") || dateStringValue.contains(":")) {
				semanticDate = Util.parseDateTime(dateStringValue);
			} else {
				semanticDate = new Date(Long.parseLong(dateStringValue));
			}

			tuple.add(semanticDate);

			if (semanticDate != null & triple.get(1) != null && triple.get(2) != null && triple.get(3) != null) {
				tuple.add(semanticDate); // put same date as end date, currently the start and end date are equal
				tuple.add(triple.get(1));
				tuple.add(triple.get(2));
				tuple.add(Util.convertStringToObject(triple.get(3)));
			} else {
				logger.info(String.format("A triple could not be read and it was ignored. Component ID: %1s",
						this.configuration.getId()));
			}

			if (lastLineRead % 30000 == 0) {
				logger.info(String.format("Read time of component %1s is %2s. Line: %3s", this.configuration.getId(),
						semanticDate.toString(), Long.valueOf(lastLineRead)));
			}

			this.lastLineRead++;
			// We emit on the default stream
			emit(tuple);
			result = true;

		} else {
			logger.info(String.format("End of file is reached in component %1s on line: %2s",
					this.configuration.getId(), lastLineRead));
			result = false;
		}

		return result;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("startDate", "endDate", "subject", "predicate", "object");
		declarer.declare(fields);
	}

}
