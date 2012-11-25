package ch.uzh.ddis.katts.bolts.output;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import ch.uzh.ddis.katts.bolts.AbstractVariableBindingsBolt;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;

public class SystemOutputBolt extends AbstractVariableBindingsBolt {
	
	private static final long serialVersionUID = 1L;
	private SystemOutputConfiguration configuration;
	private DateFormat formatter = new SimpleDateFormat("dd.MM.yyyy H:m:s");

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Event event) {
		System.out.print(event.getSequenceNumber());
		System.out.print(": \t");
		
		StreamConsumer stream = event.getEmittedOn();
		for (Variable variable : stream.getStream().getAllVariables()) {
			System.out.print("  ");
			System.out.print(variable.getName());
			System.out.print(": ");
			System.out.print(event.getVariableValue(variable));
		}
		System.out.print("    ");
		System.out.print("Event Start Date: ");
		System.out.print(formatter.format(event.getStartDate()));
		System.out.print(" ");
		System.out.print("Event End Date: ");
		System.out.print(formatter.format(event.getStartDate()));
		System.out.println();
		ack(event);
	}

	public SystemOutputConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(SystemOutputConfiguration configuration) {
		this.configuration = configuration;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	@Override
	public String getId() {
		return this.getConfiguration().getId();
	}

}
