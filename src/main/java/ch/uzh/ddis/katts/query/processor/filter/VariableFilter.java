package ch.uzh.ddis.katts.query.processor.filter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import backtype.storm.topology.IRichBolt;

import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VariableFilter extends AbstractProcessor{
	
	private Operator operator;

	@Override
	public boolean validate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Bolt getBolt() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
