package ch.uzh.ddis.katts.bolts.function;

import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.katts.bolts.AbstractVariableBindingsBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.bolts.VariableBindings;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * The expression function bolt implementation provides a bolt, which is capable to build new variable bindings. The
 * given expression is evaluated and the resulting value is assigned to the new variable. The expression must be
 * formulated in SpEL.
 * 
 * This bolt is stateless.
 * 
 * @author Thomas Hunziker
 * 
 */
public class ExpressionFunctionBolt extends AbstractVariableBindingsBolt {

	private static final long serialVersionUID = 1L;
	private ExpressionFunctionConfiguration configuration;
	private Expression expression;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		ExpressionParser parser = new SpelExpressionParser();
		expression = parser.parseExpression(this.getConfiguration().getExpression());
	}

	@Override
	public void execute(Event event) {
		Object expressionResult;
		StandardEvaluationContext context;

		context = new StandardEvaluationContext();

		// Bind all variables from the input event to the evaluation context
		for (Variable var : event.getEmittedOn().getStream().getAllVariables()) {
			context.setVariable(var.getName(), event.getVariableValue(var));
		}

		// Evaluate the expression
		expressionResult = (Object) expression.getValue(context);

		for (Stream stream : this.getStreams()) {
			VariableBindings binding = getEmitter().createVariableBindings(stream, event);

			// copy start and end date from source event
			binding.setStartDate(event.getStartDate());
			binding.setEndDate(event.getEndDate());

			// Copy Variables from the inherit stream
			for (Variable variable : stream.getInheritFrom().getAllVariables()) {
				binding.add(variable, event.getVariableValue(variable));
			}

			binding.add("result", expressionResult);

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

	@Override
	public String getId() {
		return this.getConfiguration().getId();
	}

}
