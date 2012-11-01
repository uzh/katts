package ch.uzh.ddis.katts.query.processor.filter;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class TripleCondition implements Serializable{
	
	private static final long serialVersionUID = 1L;

	public TripleCondition() {}
	
	public TripleCondition(String item, String restriction) {
		setItem(item);
		setRestriction(restriction);
	}

	@XmlAttribute(name="item")
	private String item;
	
	@XmlAttribute(name="restriction")
	private String restriction;

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
