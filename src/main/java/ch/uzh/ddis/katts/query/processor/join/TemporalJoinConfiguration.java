package ch.uzh.ddis.katts.query.processor.join;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.join.TemporalJoinBolt;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

/**
 * The temporal join joins multiple streams of variable bindings using both, their semantic and their temporal contents
 * (startDate and endDate).
 * 
 * The temporal join basically happens in two steps. First, all events arrive over the input streams are kept in the
 * join cache. The semantic join operation that checks for the actual join conditions is then executed over the data in
 * this cache. Events that do not satisfy the temporal conditions anymore are evicted from the join cache.
 * 
 * There are three steps that are being executed on the arrival of each new element in the cache. First, a configurable
 * set of eviction rules will be applied to the cache, in order to remove all data entries that have to be removed from
 * the cache <i>before</i> the join conditions are checked. Second, the actual join operation is executed, which will
 * emit all variable bindings that satisfy all join conditions. Lastly, there is a second set of eviction rules that can
 * be configured to be executed <i>after</i> the join has been executed.
 * 
 * @author Lorenz Fischer
 */
@XmlRootElement(name = "temporalJoin")
@XmlAccessorType(XmlAccessType.FIELD)
public class TemporalJoinConfiguration extends AbstractProcessor {

	private static final long serialVersionUID = 1L;

	@XmlElementWrapper(name = "evictBefore")
	@XmlElementRefs({ @XmlElementRef(type = EvictionRuleConfiguration.class) })
	private List<EvictionRuleConfiguration> evictBefore = new ArrayList<EvictionRuleConfiguration>();

	@XmlElementWrapper(name = "evictAfter")
	@XmlElementRefs({ @XmlElementRef(type = EvictionRuleConfiguration.class) })
	private List<EvictionRuleConfiguration> evictAfter = new ArrayList<EvictionRuleConfiguration>();

	@XmlElementRefs({ @XmlElementRef(type = SameValueJoinConditionConfiguration.class),
			@XmlElementRef(type = RegularJoinConditionConfiguration.class) })
	private JoinConditionConfiguration joinCondition = null;

	/**
	 * @return all the eviction rules that need to be executed <b>before</b> the join happens.
	 */
	public List<EvictionRuleConfiguration> getBeforeEvictionRules() {
		return this.evictBefore;
	}

	/**
	 * @return all the eviction rules that need to be executed <b>after</b> the join happens.
	 */
	public List<EvictionRuleConfiguration> getAfterEvictionRules() {
		return this.evictAfter;
	}

	/**
	 * @return a list of all join conditions that have to be met, in order for a variable binding set to be emitted.
	 */
	public JoinConditionConfiguration getJoinCondition() {
		return this.joinCondition;
	}

	public void setJoinCondition(JoinConditionConfiguration joinCondition) {
		this.joinCondition = joinCondition;
	}

	@Override
	public Bolt createBoltInstance() {
		return new TemporalJoinBolt(this);
	}
}
