package ch.uzh.ddis.katts.spouts.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.monitoring.StarterMonitor;
import ch.uzh.ddis.katts.query.source.File;
import ch.uzh.ddis.katts.spouts.file.source.CSVSource;
import ch.uzh.ddis.katts.spouts.file.source.GzipSourceWrapper;
import ch.uzh.ddis.katts.spouts.file.source.Source;
import ch.uzh.ddis.katts.spouts.file.source.ZipSourceWrapper;
import ch.uzh.ddis.katts.utils.Cluster;

public class FileTripleReader implements IRichSpout {

	/** This formatter is used to parse dateTime string values */
	private transient DateTimeFormatter isoFormat;

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private FileTripleReaderConfiguration configuration;
	private List<Source> sources = new ArrayList<Source>();

	@Override
	public void nextTuple() {
		final String dateStringValue;
		final Date date;
		// TODO Do synchronize the different sources
		// TODO Add support for end date

		List<String> triple = null;
		try {
			triple = sources.iterator().next().getNextTriple();
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read next triple because: %1s", e.getMessage()), e);
		}

		if (triple == null) {
			return;
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

		// currently the start and end date are equal
		tuple.add(date);
		tuple.add(triple.get(1));
		tuple.add(triple.get(2));
		tuple.add(triple.get(3));

		// We emit on the default stream, since we do not want multiple
		// streams!
		getCollector().emit(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		Fields fields = new Fields("startDate", "endDate", "subject", "predicate", "object");
		declarer.declare(fields);
	}

	@Override
	public void open(Map conf, TopologyContext aContext, SpoutOutputCollector aCollector) {
		collector = aCollector;

		buildSources();

		StarterMonitor.getInstance(conf).start();

	}

	private void buildSources() {
		for (File file : configuration.getFiles()) {
			Source source = null;
			if (file.getMimeType().equals("text/comma-separated-values")) {
				source = new CSVSource();
			}

//			if (file.isZipped()) {
//				source = new ZipSourceWrapper(source);
//			}
			
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
			this.sources.add(source);
		}

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

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public SpoutOutputCollector getCollector() {
		return collector;
	}

	public void setCollector(SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public FileTripleReaderConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(FileTripleReaderConfiguration configuration) {
		this.configuration = configuration;
	}

}
