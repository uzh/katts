package ch.uzh.ddis.katts.query;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;

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
	private String id = String.valueOf(Math.abs(Utils.randomLong()));

	@Override
	public boolean validate() throws InvalidNodeConfigurationException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void createTopology(TopologyBuilder builder) {
		
		int parallelism = getDeclaredParallelism(builder);
		
		if (this instanceof ConsumerNode) {
			Bolt bolt = ((ConsumerNode)this).getBolt();
			bolt.setConsumerStreams(((ConsumerNode)this).getConsumers());
			
			if (this instanceof ProducerNode) {
				bolt.setStreams(((ProducerNode)this).getProducers());
			}
			
			BoltDeclarer boltDeclarer = builder.setBolt(this.getId(), bolt, parallelism);
			this.attachStreams((ConsumerNode)this, boltDeclarer);
		}
		else if (this instanceof SpoutNode) {
			builder.setSpout(this.getId(), ((SpoutNode)this).getSpout(), parallelism);
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
		return this.getParallelism() > 0 ? this.getParallelism() : builder.getParallelism();
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
		}
		
	}
}
