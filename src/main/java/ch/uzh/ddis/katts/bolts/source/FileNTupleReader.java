package ch.uzh.ddis.katts.bolts.source;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.bolts.source.file.CSVSource;
import ch.uzh.ddis.katts.bolts.source.file.GzipSourceWrapper;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.bolts.source.file.ZipSourceWrapper;
import ch.uzh.ddis.katts.monitoring.StarterMonitor;
import ch.uzh.ddis.katts.query.source.File;
import ch.uzh.ddis.katts.query.source.NTupleFileSource;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

/**
 * This {@link IRichBolt} reads in n-tuples from files. Thanks to the
 * abstraction of the {@link Source} different type of files can be read with
 * this bolt.
 * 
 * Internally a thread is run to read in the data. Storm uses normally Spouts
 * for read in data. Because of the heartbeat, this must be implement as a Bolt
 * because the heartbeat is provided centrally and hence the reader must listen
 * to this stream. Spouts are not able to listen to streams. Therefore a Bolt is
 * used for this task.
 * 
 * To improve the parallelization of reading this bolt accepts multiple files.
 * For each file a separate instance is created.
 * 
 * TODO: Checkout how reliability can be achieved with this architecture.
 * 
 * @author Thomas Hunziker, Thomas Scharrenbach
 * 
 */
@SuppressWarnings("serial")
public class FileNTupleReader implements IRichBolt {

	private static final Logger _LOG = Logger.getLogger(FileNTupleReader.class
			.getCanonicalName());

	/** This formatter is used to parse dateTime string values */
	private transient DateTimeFormatter isoFormat;

	private OutputCollector collector;
	private NTupleFileSource configuration;
	private Source source = null;
	private StarterMonitor starterMonitor;

	private Thread thread = null;

	private Date currentRealTimeDate;

	private long numberRead = 0;

	private int numberOfFields;
	
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

		try {
			result = this.isoFormat.parseDateTime(value).toDate();
		} catch (IllegalArgumentException e) {
			// so it's also not a date either
		}

		try {
			result = Double.valueOf(value);
		} catch (NumberFormatException e) {
			// so it's not a double
		}

		try {
			result = Long.valueOf(value);
		} catch (NumberFormatException e) {
			// so it's not a long
		}

		return result;
	}

	/**
	 * This method reads the next tuple from the file source.
	 * 
	 * @return True, when a tuple was emitted.
	 */
	public boolean nextTuple() {
		final String dateStringValue;
		final Date date;
		// TODO Add support for end date

		List<String> nTuple = null;
		try {
			nTuple = source.getNextTuple();
		} catch (Exception e) {
			throw new RuntimeException(String.format(
					"Unable to read next triple because: %1s", e.getMessage()),
					e);
		}

		if (nTuple == null) {
			_LOG.log(
					Level.INFO,
					String.format(
							"End of file is reached in component %1s at date %2s. Line: %3s",
							this.getConfiguration().getId(),
							currentRealTimeDate.toString(), numberRead));
			return false;
		}

		// parse the date field, this supports raw millisecond values and ISO
		// formatted datetime strings
		dateStringValue = nTuple.get(0);
		if (dateStringValue.contains("-") || dateStringValue.contains("T")
				|| dateStringValue.contains(":")) {
			if (isoFormat == null) { // when the bolt gets deserialized this
										// could be reset to null
				this.isoFormat = ISODateTimeFormat.dateTimeParser();
			}
			date = this.isoFormat.parseDateTime(dateStringValue).toDate();
		} else {
			date = new Date(Long.parseLong(dateStringValue));
		}

		List<Object> tuple = new ArrayList<Object>();
		tuple.add(date);
		synchronized (this) {
			currentRealTimeDate = date;
		}

		if (date != null && !nTuple.contains(null)) {
			// currently the start and end date are equal
			tuple.add(date);
			for(String value : nTuple.subList(1, nTuple.size())) {
				tuple.add(convertStringToObject(value));				
			}

			// We emit on the default stream, since we do not want multiple
			// streams!
			synchronized (this) {
				this.collector.emit(tuple);
			}
		} else {
			_LOG.log(
					Level.INFO,
					String.format(
							"A triple could not be read and it was ignored. Component ID: %1s",
							this.getConfiguration().getId()));
		}

		if (numberRead % 30000 == 0) {
			_LOG.log(Level.INFO, String.format(
					"Read time of component %1s is %2s. Line: %3s", this
							.getConfiguration().getId(), currentRealTimeDate
							.toString(), numberRead));
		}

		numberRead++;

		return true;
	}

	@Override
	public synchronized void execute(Tuple input) {

		// This bolt receives only heart beats, hence we do not need to handle
		// here other tuples

		if (thread == null) {
			starterMonitor.start();

			// Read the first line to ensure that the currentRealTimeDate is
			// set.
			nextTuple();

			thread = new Thread(new NTupleReaderThread(this));
			thread.start();
		}

		// Ensure that the real time is not null, if so ignore the heart beat
		if (currentRealTimeDate != null) {
			List<Object> output = HeartBeatSpout.getOutputTuple(input,
					currentRealTimeDate);
			this.collector
					.emit(HeartBeatSpout
							.buildHeartBeatStreamId(getConfiguration().getId()),
							output);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> outputFieldsList = new ArrayList<String>();
		outputFieldsList.add("startDate");
		outputFieldsList.add("endDate");
		// Add the fields of the n-tuple.
		for (int i = 0; i < getNumberOfFields(); ++i) {
			outputFieldsList.add(Integer.toString(i));
		}
		Fields fields = new Fields(outputFieldsList);
		_LOG.log(Level.INFO, String.format(
				"Declaring output fields for class: %s : %s", this.getClass(),
				outputFieldsList));

		declarer.declare(fields);
		declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this
				.getConfiguration().getId()), HeartBeatSpout
				.getHeartBeatFields());
	}

	/**
	 * This method inits the source stack including the ZIP and GZIP wrappers.
	 * The determination of the required wrappers is done over the file
	 * extension.
	 * 
	 * @param sourceIndex
	 */
	private void buildSources(int sourceIndex) {
		File file = configuration.getFiles().get(sourceIndex);
		if (file.getMimeType().equals("text/comma-separated-values")) {
			source = new CSVSource();
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
			throw new RuntimeException(String.format(
					"Unable to read input file '%1s' because: %2s",
					file.getPath(), e.getMessage()), e);
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public NTupleFileSource getConfiguration() {
		return configuration;
	}

	public void setConfiguration(NTupleFileSource nTupleFileSource) {
		this.configuration = nTupleFileSource;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

		int sourceIndex = context.getThisTaskIndex();
		buildSources(sourceIndex);

		starterMonitor = StarterMonitor.getInstance(stormConf);
	}

	@Override
	public void cleanup() {
	}

	public int getNumberOfFields() {
		return numberOfFields;
	}

	public void setNumberOfFields(int numberOfFields) {
		this.numberOfFields = numberOfFields;
	}

}