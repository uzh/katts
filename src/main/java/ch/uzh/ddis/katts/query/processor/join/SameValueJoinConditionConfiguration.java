package ch.uzh.ddis.katts.query.processor.join;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.join.SameValueJoinCondition;

/**
 * This join condition checks if the value of the join field is the same for all
 * input streams.
 * 
 * @author fischer
 */
@XmlRootElement(name = "sameValue")
@XmlAccessorType(XmlAccessType.FIELD)
public class SameValueJoinConditionConfiguration implements JoinConditionConfiguration, Serializable {

	@XmlTransient
	private String joinField;

	public void setJoinField(String joinField) {
		this.joinField = joinField;
	}

	@XmlAttribute(name = "onField", required = true)
	public String getJoinField() {
		return joinField;
	}

	@Override
	public Class<SameValueJoinCondition> getImplementingClass() {
		return SameValueJoinCondition.class;
	}

	@Override
	public String toString() {
		return String.format("<sameValue onField='%1s'/>",joinField);
	}
	
}
