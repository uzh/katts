package ch.uzh.ddis.katts.query;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import ch.uzh.ddis.katts.bolts.TerminationBolt;
import ch.uzh.ddis.katts.query.processor.join.TemporalJoinConfiguration;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

/**
 * This is the XML configuration class for the {@link TerminationBolt}. 
 * 
 * @see TerminationBolt
 * 
 * @author Thomas Hunziker
 *
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Termination implements Node {
	
	private static final long serialVersionUID = 1L;
	
	@XmlTransient
	private Query query;

	@Override
	public String getId() {
		return "termination_bolt";
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
		TerminationBolt bolt = new TerminationBolt();
		BoltDeclarer boltDeclarer = topology.setBolt(this.getId(), bolt, this.getParallelism());
		
		// TODO: The attaching to each node makes no sense, because the overhead is to big. Eventually the listening node
		// should be added by configuration.

//		for (Node node : this.getQuery().getNodes()) {
//			
//			// Subscribe to all bolts in the cluster for the heart beat, to track the termination date.
//			if (!node.getId().equals(HeartBeat.HEARTBEAT_COMPONENT_ID) && !node.getId().equals(this.getId()) && !(node instanceof FileOutput)) {
//				
//				// Since this is a source, we need only to attach the heart beat stream to it.
//				boltDeclarer.allGrouping(node.getId(), HeartBeatSpout.buildHeartBeatStreamId(node.getId()));
//			}
//		}
		
		for (Node node : this.getQuery().getNodes()) {
			
			// TODO: Workaround to allow termination based on the output of the temporal join
			if (node instanceof TemporalJoinConfiguration) {
				
				// Since this is a source, we need only to attach the heart beat stream to it.
				boltDeclarer.allGrouping(node.getId(), HeartBeatSpout.buildHeartBeatStreamId(node.getId()));
			}
		}
		
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

}
