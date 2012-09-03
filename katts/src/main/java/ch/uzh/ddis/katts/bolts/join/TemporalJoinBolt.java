package ch.uzh.ddis.katts.bolts.join;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractSynchronizedBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.query.processor.join.JoinConditionConfiguration;
import ch.uzh.ddis.katts.query.processor.join.TemporalJoinConfiguration;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * The temporal join basically happens in two steps. First, all events arrive
 * over the input streams are kept in the join cache. The semantic join
 * operation that checks for the actual join conditions is then executed over
 * the data in this cache. Events that do not satisfy the temporal conditions
 * anymore are evicted from the join cache.
 * 
 * There are three steps that are being executed on the arrival of each new
 * element in the cache. First, a configurable set of eviction rules will be
 * applied to the cache, in order to remove all data entries that have to be
 * removed from the cache <i>before</i> the join conditions are checked. Second,
 * the actual join operation is executed, which will emit all variable bindings
 * that satisfy all join conditions. Lastly, there is a second set of eviction
 * rules that can be configured to be executed <i>after</i> the join has been
 * executed.
 * 
 * @author lfischer
 */
public class TemporalJoinBolt extends AbstractSynchronizedBolt {

	// TODO lorenz: use global storage facility

	/** This object holds the configuration details for this bolt. */
	private final TemporalJoinConfiguration configuration;

	/** A set containing all ids of all incoming streams. */
	private Set<String> incomingStreamIds;

	/** The join condition this bolt uses. */
	private JoinCondition joinCondition;

	/**
	 * This manager manages the eviction indices and provides methods for
	 * invoking the eviction rules.
	 */
	private EvictionRuleManager evictionRuleManager;

	/** We store all of our state variables into this object. */
	// private Storage<Object, Map<StreamConsumer, PriorityQueue<Event>>>
	// queues;
	// private Logger logger = LoggerFactory.getLogger(TemporalJoinBolt.class);

	/**
	 * Creates a new instance of this bolt type using the configuration
	 * provided.
	 * 
	 * @param configuration
	 *            the jaxb configuration object.
	 */
	public TemporalJoinBolt(TemporalJoinConfiguration configuration) {
		this.configuration = configuration;
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);

		// Create a set containing the identifiers of all incoming streams.
		this.incomingStreamIds = new HashSet<String>();
		for (StreamConsumer consumer : this.configuration.getConsumers()) {
			this.incomingStreamIds.add(consumer.getStream().getId());
		}

		// Create and configure the join condition
		if (this.configuration.getJoinCondition().size() != 1) {
			// TODO lorenz: there has to be better way to do this...
			throw new IllegalStateException("Zero or multiple join conditions configured. "
					+ "You must configure exactly ONE!");
		} else {
			JoinConditionConfiguration config;
			Constructor<? extends JoinCondition> constructor;

			// there is always exactly one condition!
			config = this.configuration.getJoinCondition().get(0);
			try {
				constructor = config.getImplementingClass().getConstructor();
				this.joinCondition = constructor.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Could not instantiate the configured join condition.", e);
			}

			this.joinCondition.prepare(config, this.incomingStreamIds);
		}

		// Create and configure the eviction rules
		this.evictionRuleManager = new EvictionRuleManager(this.configuration.getBeforeEvictionRules(),
				this.configuration.getAfterEvictionRules(), this.joinCondition, this.incomingStreamIds);
	}

	@Override
	public void execute(Event event) {
		Set<SimpleVariableBindings> joinResults;
		SimpleVariableBindings newBindings = new SimpleVariableBindings(event.getTuple());
		String streamId = event.getEmittedOn().getStream().getId();

		this.evictionRuleManager.addBindingsToIndices(newBindings, streamId);
		this.evictionRuleManager.executeBeforeEvictionRules(newBindings, streamId);
		joinResults = this.joinCondition.join(newBindings, streamId);
		this.evictionRuleManager.executeAfterEvictionRules(newBindings, streamId);

		if (joinResults.size() > 0) {
			for (SimpleVariableBindings r : joinResults) {
				System.out.println(r);
			}
		}

		ack(event);

		// emit the joined results
	}

	@Override
	public String getId() {
		return this.configuration.getId();
	}

}
