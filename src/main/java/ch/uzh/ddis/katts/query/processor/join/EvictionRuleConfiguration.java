package ch.uzh.ddis.katts.query.processor.join;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlRootElement(name = "evict")
public class EvictionRuleConfiguration implements Serializable {

	@XmlTransient
	private String from;

	@XmlTransient
	private String on;

	@XmlTransient
	private String condition;

	@XmlAttribute(name = "from", required = true)
	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	@XmlAttribute(name = "on", required = true)
	public String getOn() {
		return on;
	}

	public void setOn(String on) {
		this.on = on;
	}

	@XmlAttribute(name = "if", required = true)
	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}

	@Override
	public String toString() {
		return String.format("<evict from='%1s' on='%2s' if='%3s' />", from, on, condition);
	}
	
}
