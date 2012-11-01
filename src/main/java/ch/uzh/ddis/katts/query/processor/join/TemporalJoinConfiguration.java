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

@XmlRootElement(name = "temporalJoin")
@XmlAccessorType(XmlAccessType.FIELD)
public class TemporalJoinConfiguration extends AbstractProcessor {

	@XmlElementWrapper(name = "evictBefore")
	@XmlElementRefs({ @XmlElementRef(type = EvictionRuleConfiguration.class) })
	private List<EvictionRuleConfiguration> evictBefore = new ArrayList<EvictionRuleConfiguration>();

	@XmlElementWrapper(name = "evictAfter")
	@XmlElementRefs({ @XmlElementRef(type = EvictionRuleConfiguration.class) })
	private List<EvictionRuleConfiguration> evictAfter = new ArrayList<EvictionRuleConfiguration>();

	@XmlElementRefs({ 
		@XmlElementRef(type = SameValueJoinConditionConfiguration.class),
		@XmlElementRef(type = RegularJoinConditionConfiguration.class)
		})
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
	public Bolt getBolt() {
		return new TemporalJoinBolt(this);
	}
}
