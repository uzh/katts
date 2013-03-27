package ch.uzh.ddis.katts.bolts.source;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.source.file.PrefetchSourceWrapper;
import ch.uzh.ddis.katts.monitoring.StarterMonitor;
import ch.uzh.ddis.katts.query.source.CachedFileSourceConfiguration;

/**
 * @see CachedFileSource
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class CachedFileTripleReader extends FileTripleReader {

	// private Logger logger = LoggerFactory.getLogger(CachedFileTripleReader.class);

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		int numberOfTasks; // the number of task instances that have been created by the system
		int thisTaskId;
		CachedFileSourceConfiguration xmlConfiguration;
		
		if (getConfiguration() instanceof CachedFileSourceConfiguration) {
			xmlConfiguration = (CachedFileSourceConfiguration) getConfiguration();
		} else {
			throw new IllegalArgumentException("Something is wrong with your configuration");
		}
		
		// check configuration
		if (getConfiguration().getFiles().size() != 1) {
			throw new IllegalArgumentException("Misconfiguration in CachedFileSource: only one file allowed!");
		}

		// init variables for super class - ugly
		setCollector(collector);
		setStarterMonitor(StarterMonitor.getInstance(stormConf));		
		
		// read out how many task instances storm created
		numberOfTasks = context.getComponentTasks(context.getThisComponentId()).size();
		thisTaskId = context.getThisTaskIndex();

		// sanity check - the number of task instances and the declared parallelism should be the same
		if (xmlConfiguration.getParallelism() != numberOfTasks) {
			throw new IllegalStateException(String.format(
					"Parallelism (%d) and numberOfTasks (%d) should be the same, but they are not!",
					xmlConfiguration.getParallelism(), numberOfTasks));
		}

		// build sources as we could regularly. Note that we only have one file configured at position 0
		buildSources(0);
		// wrap the source
		try {
			setSource(new PrefetchSourceWrapper(getSource(), xmlConfiguration.getBlockSize(), numberOfTasks,
					thisTaskId));
		} catch (Exception e) {
			throw new RuntimeException("Error while prefetching the file source", e);
		}

	}

	@Override
	public void cleanup() {
	}

}
