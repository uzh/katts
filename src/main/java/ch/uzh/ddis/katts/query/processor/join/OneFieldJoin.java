package ch.uzh.ddis.katts.query.processor.join;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.join.OneFieldJoinBolt;
import ch.uzh.ddis.katts.bolts.join.OneFieldJoinConfiguration;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;
import ch.uzh.ddis.katts.query.stream.Variable;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class OneFieldJoin extends AbstractProcessor implements OneFieldJoinConfiguration{

	private static final long serialVersionUID = 1L;
	
	@XmlTransient
	private Variable joinOn;
	
	@XmlTransient
	private int maxBufferSize = 20;
	
	@XmlTransient
	private long joinPrecision = 20000;

	@Override
	public Bolt getBolt() {
		OneFieldJoinBolt bolt = new OneFieldJoinBolt();
		bolt.setConfiguration(this);
		return bolt;
	}

	@XmlIDREF
	@XmlAttribute(name="joinOn", required=true)
	@Override
	public Variable getJoinOn() {
		return joinOn;
	}

	public void setJoinOn(Variable joinOn) {
		this.joinOn = joinOn;
	}
	
	@XmlAttribute(name="maxBufferSize")
	@Override
	public int getMaxBufferSize() {
		return maxBufferSize;
	}

	public void setMaxBufferSize(int maxBufferSize) {
		this.maxBufferSize = maxBufferSize;
	}

	@XmlAttribute(name="joinPrecision")
	@Override
	public long getJoinPrecision() {
		return joinPrecision;
	}

	public void setJoinPrecision(long joinPrecision) {
		this.joinPrecision = joinPrecision;
	}


}
