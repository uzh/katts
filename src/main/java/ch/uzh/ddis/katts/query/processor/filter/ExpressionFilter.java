package ch.uzh.ddis.katts.query.processor.filter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.filter.ExpressionFilterBolt;
import ch.uzh.ddis.katts.bolts.filter.ExpressionFilterConfiguration;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

/**
 * This class handles the XML configuration of the {@link ExpressionFilterBolt}. The expression function configuration
 * node does only provides a expression in SpEL.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ExpressionFilter extends AbstractProcessor implements ExpressionFilterConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlTransient
	private String expression;

	@Override
	public Bolt getBolt() {
		ExpressionFilterBolt bolt = new ExpressionFilterBolt();
		bolt.setConfiguration(this);
		return bolt;
	}

	@XmlAttribute(name = "expression", required = true)
	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

}
