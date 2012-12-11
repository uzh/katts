package ch.uzh.ddis.katts.query.processor.filter;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * This class represents the triple filter condition in XML. The condition consists of the item (subject,predicate or
 * object) and the condition that this item must meet to be processed.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TripleCondition implements Serializable {

	private static final long serialVersionUID = 1L;

	public TripleCondition() {
	}

	public TripleCondition(String item, String restriction) {
		setItem(item);
		setRestriction(restriction);
	}

	@XmlAttribute(name = "item")
	private String item;

	@XmlAttribute(name = "restriction")
	private String restriction;

	/**
	 * The item that must be filtered. This can be the subject, predicate or the object.
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
