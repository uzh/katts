package ch.uzh.ddis.katts.bolts.source;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.bolts.source.file.CSVSource;
import ch.uzh.ddis.katts.bolts.source.file.GzipSourceWrapper;
import ch.uzh.ddis.katts.bolts.source.file.N5Source;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.bolts.source.file.ZipSourceWrapper;
import ch.uzh.ddis.katts.monitoring.StarterMonitor;
import ch.uzh.ddis.katts.query.source.File;
import ch.uzh.ddis.katts.utils.Util;

/**
 * This Spout reads in triples from files. By the abstraction of the {@link Source} different type of files can be read
 * with this spout.
 * 
 * To improve the parallelization of reading this spout can be configured with multiple files. For each file a separate
 * instance is created.
 * 
 * TODO: Checkout how reliability can be achieved with this architecture.
 * 
 * @author Thomas Hunziker
 * 
 */
public class FileTripleReader implements IRichSpout {

	/** This formatter is used to parse dateTime string values */
	private transient DateTimeFormatter isoFormat = ISODateTimeFormat.dateTimeParser();

	private static final long serialVersionUID = 1L;

	/** We emit tuples over this collector. */
	private SpoutOutputCollector collector;

	private FileTripleReaderConfiguration configuration;
	private Source source = null;

	private Logger logger = LoggerFactory.getLogger(FileTripleReader.class);

	private long numberRead = 0;

	/**
	 * Converts a string into a proper Java object.
	 * <p/>
	 * The order in which we try to convert the values is: Long - Double - Date - String.
	 * 
	 * @param value
	 *            the string value to convert into an object.
	 * @return the created object.
	 */
	public Object convertStringToObject(String value) {
		Object result = value;

		if (Util.isLong(value)) {
			result = Long.valueOf(value);
		} else if (Util.isDouble(value)) {
			result = Double.valueOf(value);
		} else if (Util.isIsoDate(value)) {
			result = this.isoFormat.parseDateTime(value).toDate();
		}

		return result;
	}

	/**
	 * This method reads the next tuple from the file source.
	 * 
	 * @return True, when a tuple was emitted.
	 */
	public void nextTuple() {
		List<String> triple = null;

		try {
			triple = source.getNextTuple();
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read next triple because: %1s", e.getMessage()), e);
		}

		if (triple == null) {
			final String eof = "eof";
			logger.info(String.format("End of file is reached in component %1s on line: %2s", this.getConfiguration()
					.getId(), numberRead));
			// /*
			// * TODO: this is a dirty hack: by setting the process date set to null, we tell all the following bolts
			// that
			// * we are done with reading from this file.
			// */
			// tuple.add(null);
			// tuple.add(eof);
			// tuple.add(eof);
			// tuple.add(eof);
		} else {
			List<Object> tuple = new ArrayList<Object>();
			Date semanticDate;
			final String dateStringValue;

			// parse the date field, this supports raw millisecond values and ISO formatted datetime strings
			dateStringValue = triple.get(0);
			if (dateStringValue.contains("-") || dateStringValue.contains("T") || dateStringValue.contains(":")) {
				if (this.isoFormat == null) {
					this.isoFormat = ISODateTimeFormat.dateTimeParser();
				}
				semanticDate = this.isoFormat.parseDateTime(dateStringValue).toDate();
			} else {
				semanticDate = new Date(Long.parseLong(dateStringValue));
			}

			tuple.add(semanticDate);

			if (semanticDate != null & triple.get(1) != null && triple.get(2) != null && triple.get(3) != null) {
				tuple.add(semanticDate); // put same date as end date, currently the start and end date are equal
				tuple.add(triple.get(1));
				tuple.add(triple.get(2));
				tuple.add(convertStringToObject(triple.get(3)));
			} else {
				logger.info(String.format("A triple could not be read and it was ignored. Component ID: %1s", this
						.getConfiguration().getId()));
			}

			if (numberRead % 30000 == 0) {
				logger.info(String.format("Read time of component %1s is %2s. Line: %3s", this.getConfiguration()
						.getId(), semanticDate.toString(), numberRead));
			}

			numberRead++;

			// We emit on the default stream
			this.collector.emit(tuple);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("startDate", "endDate", "subject", "predicate", "object");
		declarer.declare(fields);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		// getThisTaskIndex returns the index of this task among all tasks for this component
		buildSources(context.getThisTaskIndex());

		StarterMonitor.getInstance(conf).start();
	}

	/**
	 * This method inits the source stack including the ZIP and GZIP wrappers. The determination of the required
	 * wrappers is done over the file extension.
	 * 
	 * @param sourceIndex
	 */
	protected void buildSources(int sourceIndex) {
		File file = configuration.getFiles().get(sourceIndex);
		if (file.getMimeType().equals("text/comma-separated-values")) {
			source = new CSVSource();
		} else if (file.getMimeType().equals("text/n5")) {
			source = new N5Source();
		}

		if (file.getPath().endsWith(".zip")) {
			source = new ZipSourceWrapper(source);
		} else if (file.getPath().endsWith(".gz")) {
			source = new GzipSourceWrapper(source);
		}

		try {
			InputStream inputStream = source.buildInputStream(file);
			source.setFileInputStream(inputStream);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read input file '%1s' because: %2s", file.getPath(),
					e.getMessage()), e);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public FileTripleReaderConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(FileTripleReaderConfiguration configuration) {
		this.configuration = configuration;
	}

	/**
	 * {@link FileTripleReader#source}
	 * 
	 * @return the source
	 */
	protected Source getSource() {
		return source;
	}

	/**
	 * {@link FileTripleReader#source}
	 * 
	 * @param source
	 *            the source to set
	 */
	protected void setSource(Source source) {
		this.source = source;
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object msgId) {
	}

	@Override
	public void fail(Object msgId) {
	}

}
