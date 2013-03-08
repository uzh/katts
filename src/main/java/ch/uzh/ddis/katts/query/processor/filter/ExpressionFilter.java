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
 * ExpressionFilters apply a filter on every variable binding that is received on either of the configured input
 * streams. If the expression matches the contents of the variable binding, it is sent onwards. If the expression does
 * not match, the variable binding is dropped from the stream.
 * 
 * The expressions are specified in the Spring expression language SpEL
 * (http://static.springsource.org/spring/docs/3.0.x/reference/expressions.html). Fields can be referenced using the
 * following Notation: #{Name_Of_Variable}.
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
	public Bolt createBoltInstance() {
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
