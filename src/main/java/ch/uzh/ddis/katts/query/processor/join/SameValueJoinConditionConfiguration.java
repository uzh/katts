package ch.uzh.ddis.katts.query.processor.join;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.join.SameValueJoinCondition;

/**
 * This join condition checks if the value of ll the join fields are the same on all incoming streams. The value of the joinFields
 * attribute is a comma separated list of field names.
 * 
 * <p/>Example:
 * &lt;sameValue onFields="ticker_id,ticker_department" /&gt;
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * @see TemporalJoinConfiguration
 */
@XmlRootElement(name = "sameValue")
@XmlAccessorType(XmlAccessType.FIELD)
public class SameValueJoinConditionConfiguration implements JoinConditionConfiguration, Serializable {

	private static final long serialVersionUID = 1L;
	
	@XmlTransient
	private String joinFields;

	public void setJoinFields(String joinField) {
		this.joinFields = joinField;
	}

	@XmlAttribute(name = "onFields", required = true)
	public String getJoinFields() {
		return joinFields;
	}

	@Override
	public Class<SameValueJoinCondition> getImplementingClass() {
		return SameValueJoinCondition.class;
	}

	@Override
	public String toString() {
		return String.format("<sameValue onField='%1s'/>",joinFields);
	}
	
}
