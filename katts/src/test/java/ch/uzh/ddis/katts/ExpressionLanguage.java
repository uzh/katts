package ch.uzh.ddis.katts;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import ch.uzh.ddis.katts.bolts.function.ExpressionFunctionConfiguration;
import ch.uzh.ddis.katts.query.stream.Variable;

public class ExpressionLanguage {

	public static void main(String[] args) {
		ExpressionParser parser = new SpelExpressionParser();
		Expression expression = parser.parseExpression("#ticker_min / #ticker_max");
		
		
		StandardEvaluationContext context = new StandardEvaluationContext();
		
		context.setVariable("ticker_min", 14d);
		context.setVariable("ticker_max", 1d);
		
		
		Object result = (Object) expression.getValue(context);
	
		
		System.out.println(result);
		
	}
	
}
