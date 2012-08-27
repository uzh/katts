package ch.uzh.ddis.katts.spouts.file;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import ch.uzh.ddis.katts.query.source.File;
import ch.uzh.ddis.katts.spouts.file.source.CSVSource;
import ch.uzh.ddis.katts.spouts.file.source.Source;
import ch.uzh.ddis.katts.spouts.file.source.ZipSourceWrapper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class FileTripleReader implements IRichSpout{

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	private FileTripleReaderConfiguration configuration;
	private List<Source> sources = new ArrayList<Source>();
	
	@Override
	public void nextTuple() {
		
		// TODO Do synchronize the different sources
		// TODO Add support for end date
		
		List<String> triple = null;
		try {
			triple = sources.iterator().next().getNextTriple();
			if (triple == null) {
				return;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		long time = Long.parseLong(triple.get(0));
		Date date = new Date(time);
		
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
	public void open(Map conf, TopologyContext aContext,
			SpoutOutputCollector aCollector) {
		collector = aCollector;
		
		buildSources();
		
	}

	private void buildSources() {
		for (File file : configuration.getFiles()) {
			Source source = null;
			if (file.getMimeType().equals("text/comma-separated-values")) {
				source = new CSVSource();
			}
			
			if (file.isZipped()) {
				source = new ZipSourceWrapper(source);
			}
			try {
				InputStream inputStream = source.buildInputStream(file);
				source.setFileInputStream(inputStream);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.sources.add(source);
		}
		
	}


	@Override
	public void close() {}

	@Override
	public void activate() {}

	@Override
	public void deactivate() {}

	@Override
	public void ack(Object msgId) {}

	@Override
	public void fail(Object msgId) {}

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
