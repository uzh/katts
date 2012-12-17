package ch.uzh.ddis.katts.query.processor.function;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.function.ExpressionFunctionBolt;
import ch.uzh.ddis.katts.bolts.function.ExpressionFunctionConfiguration;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

/**
 * ExpressionFunction allow the user to create new variables based on the values of other variables in a variable
 * binding. It evaluates an expression and assign its value to a reference name called "result". This reference can be
 * used in the produces clause of the configuration to assign the resulting value to a variable name of the outgoing
 * stream.
 * 
 * The expressions are specified in the Spring expression language SpEL
 * (http://static.springsource.org/spring/docs/3.0.x/reference/expressions.html). Fields can be referenced using the
 * following Notation: #{Name_Of_Variable}.
 * 
 * @author Thomas Hunziker
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ExpressionFunction extends AbstractProcessor implements ExpressionFunctionConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlTransient
	private String expression;

	@Override
	public Bolt getBolt() {
		ExpressionFunctionBolt bolt = new ExpressionFunctionBolt();
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
