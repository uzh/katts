package ch.uzh.ddis.katts.query;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.query.source.Source;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;
import ch.uzh.ddis.katts.spouts.file.HeartBeatSpout;

/**
 * The AbstractNode implements some convenient methods for nodes.
 * It provides functionality to link the different nodes together with
 * their streams and to init the different node implementation.
 * 
 * @author Thomas Hunziker
 *
 */
public abstract class AbstractNode implements Node{
	
	private static final long serialVersionUID = 1L;
	
	@XmlAttribute(required=false)
	private String id = String.valueOf(Math.abs(Utils.secureRandomLong()));
	
	@XmlTransient
	private Query query;

	@Override
	public boolean validate() throws InvalidNodeConfigurationException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void createTopology(TopologyBuilder builder) {
		
		int parallelism = getDeclaredParallelism(builder);
		
		// does this work with nodes that only produce??
		if (this instanceof ConsumerNode) {
			Bolt bolt = ((ConsumerNode)this).getBolt();
			bolt.setConsumerStreams(((ConsumerNode)this).getConsumers());
			
			if (this instanceof ProducerNode) {
				bolt.setStreams(((ProducerNode)this).getProducers());
			}
			
			BoltDeclarer boltDeclarer = builder.setBolt(this.getId(), bolt, parallelism);
			this.attachStreams((ConsumerNode)this, boltDeclarer);
		}
		else if (this instanceof Source) {
			BoltDeclarer boltDeclarer = builder.setBolt(this.getId(), ((Source)this).getBolt(), parallelism);
			
			// Since this is a source, we need only to attach the heart beat stream to it.
			boltDeclarer.allGrouping(HeartBeat.HEARTBEAT_COMPONENT_ID, HeartBeatSpout.HEARTBEAT_STREAMID);
		}
	}
	
	/**
	 * This method returns the effective parallelism value set in the
	 * storm topology. 
	 * 
	 * @param builder
	 * @return
	 */
	public int getDeclaredParallelism(TopologyBuilder builder) {
		return this.getParallelism() > 0 ? this.getParallelism() : Math.round((float)builder.getParallelism() * builder.getParallelizationWeightByNode(this));
	}

	@Override
	@XmlTransient
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	/**
	 * This method links the different nodes together. 
	 * 
	 * @param node The node that should be linked to the topology
	 * @param bolt The storm bolt declaration object.
	 */
	public void attachStreams(ConsumerNode node, BoltDeclarer bolt) {
		for (StreamConsumer stream : node.getConsumers()) {
			stream.getGrouping().attachToBolt(bolt, stream);
			
			// Attach the heart beat to the bolt
			String sourceComponentId = stream.getStream().getNode().getId();
			bolt.allGrouping(sourceComponentId, HeartBeatSpout.buildHeartBeatStreamId(sourceComponentId));
		}
		
		
		
	}

	@Override
	@XmlTransient
	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}
}
