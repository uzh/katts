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
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

public class FileTripleReader implements IRichBolt {

	/** This formatter is used to parse dateTime string values */
	private transient DateTimeFormatter isoFormat;
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private FileTripleReaderConfiguration configuration;
	private Source source = null;
	private StarterMonitor starterMonitor;
	
	private Thread thread = null;
	
	private TripleReaderThread thripleReaderThread;
	
	private Date currentRealTimeDate;
	
	private Logger logger = LoggerFactory.getLogger(FileTripleReader.class);
	
	private long numberRead = 0;

	public boolean nextTuple() {
		final String dateStringValue;
		final Date date;
		// TODO Add support for end date

		List<String> triple = null;
		try {
			triple = source.getNextTriple();
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read next triple because: %1s", e.getMessage()), e);
		}

		if (triple == null) {
			logger.info(String.format("End of file is reached in component %1s at date %2s. Line: %3s", this.getConfiguration().getId(), currentRealTimeDate.toString(), numberRead));
			return false;
		}

		// parse the date field, this supports raw millisecond values and ISO formatted datetime strings
		dateStringValue = triple.get(0);
		if (dateStringValue.contains("-") || dateStringValue.contains("T") || dateStringValue.contains(":")) {
			if (isoFormat == null) { // when the bolt gets deserialized this could be reset to null
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

		if (date != null & triple.get(1) != null && triple.get(2) != null && triple.get(3) != null) {
			// currently the start and end date are equal
			tuple.add(date);
			tuple.add(triple.get(1));
			tuple.add(triple.get(2));
			tuple.add(triple.get(3));
			
			// We emit on the default stream, since we do not want multiple
			// streams!
			synchronized (this) {
				getCollector().emit(tuple);
			}
		}
		else {
			logger.info(String.format("A triple could not be read and it was ignored. Component ID: %1s", this.getConfiguration().getId()));
		}
		
		if (numberRead % 30000 == 0) {
			logger.info(String.format("Read time of component %1s is %2s. Line: %3s", this.getConfiguration().getId(), currentRealTimeDate.toString(), numberRead));
		}
		
		numberRead++;

		return true;
	}

	@Override
	public synchronized void execute(Tuple input) {
		
		// This bolt receives only heart beats, hence we do not need to handle here other tuples
		
		if (thread == null) {	
			starterMonitor.start();
			
			// Read the first line to ensure that the currentRealTimeDate is set.
			nextTuple();
			
			thread = new Thread(new TripleReaderThread(this));
			thread.start();
		}
		
		// Ensure that the real time is not null, if so ignore the heart beat
		if (currentRealTimeDate != null) {
			List<Object> output = HeartBeatSpout.getOutputTuple(input, currentRealTimeDate);
			getCollector().emit(HeartBeatSpout.buildHeartBeatStreamId(getConfiguration().getId()), output);
		}
	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("startDate", "endDate", "subject", "predicate", "object");
		declarer.declare(fields);
		declarer.declareStream(HeartBeatSpout.buildHeartBeatStreamId(this.getConfiguration().getId()), HeartBeatSpout.getHeartBeatFields());
	}


	private void buildSources(int sourceIndex) {
		File file = configuration.getFiles().get(sourceIndex);
		if (file.getMimeType().equals("text/comma-separated-values")) {
			source = new CSVSource();
		}

		if (file.getPath().endsWith(".zip")) {
			source = new ZipSourceWrapper(source);
		}
		else if(file.getPath().endsWith(".gz")) {
			source = new GzipSourceWrapper(source);
		}
		
		try {
			InputStream inputStream = source.buildInputStream(file);
			source.setFileInputStream(inputStream);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read input file '%1s' because: %2s",
					file.getPath(), e.getMessage()), e);
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

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		int sourceIndex = context.getThisTaskIndex();
		buildSources(sourceIndex);
		
		starterMonitor = StarterMonitor.getInstance(stormConf);
	}

	@Override
	public void cleanup() {
	}

	public synchronized OutputCollector getCollector() {
		return collector;
	}

	public synchronized void setCollector(OutputCollector collector) {
		this.collector = collector;
	}
	
	
}
