package ch.uzh.ddis.katts.query.processor.join;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.join.RegularJoinCondition;

/**
 * This join condition checks if the value of the join field is the same for all input streams.
 * 
 * @author fischer
 */
@XmlRootElement(name = "regularJoin")
@XmlAccessorType(XmlAccessType.FIELD)
public class RegularJoinConditionConfiguration implements JoinConditionConfiguration, Serializable {

	@XmlElementRefs({ @XmlElementRef(type = JoinVariableConfiguration.class) })
	private List<JoinVariableConfiguration> joinVariables = new ArrayList<JoinVariableConfiguration>();

	public List<JoinVariableConfiguration> getJoinVariables() {
		return joinVariables;
	}

	public void setJoinVariables(List<JoinVariableConfiguration> joinVariables) {
		this.joinVariables = joinVariables;
	}

	@Override
	public Class<RegularJoinCondition> getImplementingClass() {
		return RegularJoinCondition.class;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();

		result.append("<" + getClass().getAnnotation(XmlRootElement.class).name() + ">");
		for (JoinVariableConfiguration joinVariable : this.joinVariables) {
			result.append("\t").append(joinVariable.toString());
		}
		result.append("</" + getClass().getAnnotation(XmlRootElement.class).name() + ">");

		return result.toString();
	}

}
