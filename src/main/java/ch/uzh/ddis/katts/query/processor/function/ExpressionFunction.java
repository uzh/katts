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

	@XmlAttribute(name="expression", required=true)
	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

}
