package ch.uzh.ddis.katts.bolts.source;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.bolts.source.file.CSVSource;
import ch.uzh.ddis.katts.bolts.source.file.GzipSourceWrapper;
import ch.uzh.ddis.katts.bolts.source.file.N5Source;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.bolts.source.file.ZipSourceWrapper;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;
import ch.uzh.ddis.katts.query.source.File;

/**
 * This class can be extended by Spouts that also need to read one or multiple files that are either plain text or
 * zipped (zip/gzip).
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public abstract class AbstractLineReader implements IRichSpout {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	/** We emit tuples over this collector. */
	private SpoutOutputCollector collector;

	/** We use this reference to signal that we're done processing. */
	private TerminationMonitor terminationMonitor;

	/** This is the file source this spout reads from. */
	private Source source = null;

	/*
	 * We count each emitted message and have counts for the acked and failed messages. When we are done reading from
	 * the file and the number of acked and failed messages add up to the number of emitted messages, we know that we
	 * are done processing the input file. We don't re-send messages if they have failed, but merely write a warning to
	 * the log.
	 * 
	 * According to this thread:
	 * https://groups.google.com/forum/?fromgroups=#!searchin/storm-user/thread$20ack$20fail$20nextTuple
	 * /storm-user/kM2O1gT4lPs/RJy64AcQm4QJ nextTuple, ack and fail are all called on the the same thread, so we don't
	 * need thread safe counters.
	 */

	/** Number of emitted messages. */
	private long emitted = 0;

	/** Number of acked messages. */
	private long acked = 0;

	/** Number of failed messages. */
	private long failed = 0;

	/** True, if the end of the file (we're currently reading) has been reached, false otherwise. */
	private boolean eofReached = false;

	/** The basic configuration necessary for a line reader. */
	private FileTripleReaderConfiguration configuration;

	public AbstractLineReader(FileTripleReaderConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.terminationMonitor = TerminationMonitor.getInstance(conf);

		// getThisTaskIndex returns the index of this task among all tasks for this component
		this.source = buildSources(context.getThisTaskIndex());
		this.terminationMonitor.registerSource(this.source.getSourceId());
	}

	/**
	 * This method reads the next tuple from the file source.
	 * 
	 * @return True, when a tuple was emitted.
	 */
	public final void nextTuple() {
		if (!this.eofReached) {
			if (!nextTuple(this.source)) {
				this.eofReached = true;
				checkForCompletion();
			}
		} else {
			try {
				// as requested by ISpout#nextTuple() documentation
				Thread.sleep(1);
			} catch (InterruptedException e) {
				logger.warn("Interrupted while sleeping for a millisecond (eof reached)");
			}
		}
	}

	/**
	 * This method will be called from the {@link #nextTuple()} method of this class using the configured source object.
	 * 
	 * @param source
	 *            the source objects from which the next tuple(s) should be read out of.
	 * @return true if the next tuple could be emitted, false if we reached the end of the input (eof).
	 */
	protected abstract boolean nextTuple(Source source);

	/**
	 * Emits the supplied tuple on the default stream.
	 * 
	 * @param tuple
	 *            the tuple to emit.
	 */
	protected void emit(List<Object> tuple) {
		emit(Utils.DEFAULT_STREAM_ID, tuple);
	}

	/**
	 * Emits the supplied tuple on the given stream.
	 * 
	 * @param streamId
	 *            the name of the stream to emit the tuple on
	 * @param tuple
	 *            the tuple to emit.
	 */
	protected void emit(String streamId, List<Object> tuple) {
		/*
		 * We only start measuring time, when the first tuple gets emitted. By doing this, the first call of
		 * #nextTuple(Source) can take a long time without this counting towards the runtime measurement.
		 */
		if (this.emitted == 0) {
			this.terminationMonitor.start();
		}

		// We emit on the default stream
		this.collector.emit(streamId, tuple, ++this.emitted);
	}

	/**
	 * This method inits the source stack including the ZIP and GZIP wrappers. The determination of the required
	 * wrappers is done over the file extension.
	 * 
	 * @param sourceIndex
	 */
	protected Source buildSources(int sourceIndex) {
		Source src;

		File file = this.configuration.getFiles().get(sourceIndex);

		if (file.getMimeType().equals("text/comma-separated-values")) {
			src = new CSVSource(file.getReadToLineNo());
		} else if (file.getMimeType().equals("text/n5")) {
			src = new N5Source(file.getReadToLineNo());
		} else {
			throw new IllegalArgumentException("The file is neither csv nor n5. We only support these two.");
		}

		if (file.getPath().endsWith(".zip")) {
			src = new ZipSourceWrapper(src);
		} else if (file.getPath().endsWith(".gz")) {
			src = new GzipSourceWrapper(src);
		}

		try {
			InputStream inputStream = src.buildInputStream(file);
			src.setFileInputStream(inputStream);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Unable to read input file '%1s' because: %2s", file.getPath(),
					e.getMessage()), e);
		}

		return src;
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
		this.acked++;
		checkForCompletion();
	}

	@Override
	public void fail(Object msgId) {
		logger.warn("The message " + msgId.toString() + " or source " + this.source.getSourceId()
				+ " has failed to be processed.");
		this.failed++;
		checkForCompletion();
	}

	/**
	 * This method checks if we have reached the end of the file we're currently reading and if all emitted messages
	 * have either been acked or failed. If so, the method informs the monitoring facility of this fact.
	 */
	private void checkForCompletion() {
		if (this.eofReached && (this.acked + this.failed == this.emitted)) {
			this.terminationMonitor.terminate(this.source.getSourceId());
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
