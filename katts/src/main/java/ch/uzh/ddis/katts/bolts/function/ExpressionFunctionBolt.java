package ch.uzh.ddis.katts.bolts.function;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

public class ExpressionFunctionBolt extends AbstractBolt{

	private static final long serialVersionUID = 1L;
	private ExpressionFunctionConfiguration configuration;
	private DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy H:m:s");
	private Expression expression;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		ExpressionParser parser = new SpelExpressionParser();
		expression = parser.parseExpression(this.getConfiguration().getExpression());
	}
	
	@Override
	public void execute(Event event) {
		
		for (Stream stream : this.getStreams()) {
			VariableBindings binding = getEmitter().createVariableBindings(stream, event);

			// Copy Variables from the inherit stream
			for (Variable variable : stream.getInheritFrom().getAllVariables()) {
				binding.add(variable, event.getVariableValue(variable));
			}
			
			StandardEvaluationContext context = new StandardEvaluationContext();
			
			for (Variable var : event.getEmittedOn().getStream().getAllVariables()) {
				context.setVariable(var.getName(), event.getVariableValue(var));
			}
			
			Object result = (Object) expression.getValue(context);
			binding.add("result", result);
			
			binding.emit();
		}
		
		event.ack();
	}

	public ExpressionFunctionConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(ExpressionFunctionConfiguration configuration) {
		this.configuration = configuration;
	}

}
