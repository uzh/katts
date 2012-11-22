package ch.uzh.ddis.katts.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;
import ch.uzh.ddis.katts.spouts.file.HeartBeatConfiguration;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class HeartBeat implements Node, HeartBeatConfiguration {

	private static final long serialVersionUID = 1L;
	
	public static final String HEARTBEAT_COMPONENT_ID = "heartbeat";

	@XmlTransient
	private long duration;
	
	@XmlTransient
	private Query query;

	@Override
	public String getId() {
		return HEARTBEAT_COMPONENT_ID;
	}

	@Override
	public boolean validate() throws InvalidNodeConfigurationException {
		return true;
	}

	@Override
	public int getParallelism() {
		return 1;
	}

	@Override
	public void createTopology(TopologyBuilder topology) {
		
		HeartBeatSpout spout = new HeartBeatSpout();
		spout.setConfiguration(this);
		
		topology.setSpout(this.getId(), spout);
		
	}

	@Override
	@XmlTransient
	public Query getQuery() {
		return this.query;
	}

	@Override
	public void setQuery(Query query) {
		this.query = query;
	}

	@Override
	@XmlAttribute(name="interval", required=true)
	public long getHeartBeatInterval() {
		return this.duration;
	}

	public void setHeartBeatInverval(long duration) {
		this.duration = duration;
	}
	
	
	
}
