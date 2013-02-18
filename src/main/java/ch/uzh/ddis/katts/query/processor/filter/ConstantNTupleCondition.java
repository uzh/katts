package ch.uzh.ddis.katts.query.processor.filter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.tuple.Tuple;

/**
 * 
 * 
 * @author Thomas Scharrenbach
 * 
 */
@SuppressWarnings("serial")
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ConstantNTupleCondition implements NTupleCondition {

	public ConstantNTupleCondition() {
	}

	@Override
	public boolean matches(Tuple tuple) {
		String value = tuple.getStringByField(getItem());
		return value.equals(getRestriction());
	}

	public ConstantNTupleCondition(String item, String restriction) {
		setItem(item);
		setRestriction(restriction);
	}

	@XmlAttribute(name = "item")
	private String item;

	@XmlAttribute(name = "restriction")
	private String restriction;

	/**
	 * The field which must match the restriction.
	 * 
	 * @return
	 */
	@XmlTransient
	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	@XmlTransient
	public String getRestriction() {
		return restriction;
	}

	public void setRestriction(String restriction) {
		this.restriction = restriction;
	}

}
