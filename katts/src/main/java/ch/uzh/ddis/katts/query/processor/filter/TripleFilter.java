package ch.uzh.ddis.katts.query.processor.filter;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.TopologyBuilder;
import ch.uzh.ddis.katts.bolts.filter.TripleFilterBolt;
import ch.uzh.ddis.katts.bolts.filter.TripleFilterConfiguration;
import ch.uzh.ddis.katts.query.AbstractNode;
import ch.uzh.ddis.katts.query.Node;
import ch.uzh.ddis.katts.query.ProducerNode;
import ch.uzh.ddis.katts.query.source.Source;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;

/**
 * A triple filter does build the variable bindings from a triple stream (output
 * of an {@link Source} {@link Node}.
 * 
 * The filtering can be done based on some conditions (see
 * {@link TripleFilterBolt#setConditions(List)}).
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
public class TripleFilter extends AbstractNode implements ProducerNode, TripleFilterConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlAttribute(name = "applyOnSource")
	private String applyOnSource;

	@XmlAttribute(name = "groupOn")
	private String groupOn;

	@XmlElementWrapper(name = "conditions")
	@XmlElement(name = "condition")
	private List<TripleCondition> conditions = new ArrayList<TripleCondition>();

	@XmlTransient
	private List<Stream> producers = new ArrayList<Stream>();

	@XmlElementWrapper(name = "produces")
	@XmlElement(name = "stream")
	@Override
	public List<Stream> getProducers() {
		return producers;
	}

	public void setProducers(List<Stream> producers) {
		this.producers = producers;
		for (Stream p : this.producers) {
			p.setNode(this);
		}
	}

	public void appendProducer(Stream producer) {
		producer.setNode(this);
		this.getProducers().add(producer);
	}

	@Override
	public void createTopology(TopologyBuilder builder) {
		TripleFilterBolt bolt = new TripleFilterBolt();
		bolt.setConfiguration(this);
		BoltDeclarer declarer = builder.setBolt(this.getId(), bolt, getDeclaredParallelism(builder));
		declarer.fieldsGrouping(applyOnSource, new Fields(groupOn));
	}

	@Override
	public int getParallelism() {
		return 0;
	}

	@Override
	public boolean validate() throws InvalidNodeConfigurationException {
		if (!groupOn.equals("subject") || !groupOn.equals("object") || !groupOn.equals("predicate")) {
			throw new InvalidNodeConfigurationException(
					"The TripleFilterBolt can group the source stream only by 'subject', 'object' or 'predicate'");
		}
		return true;
	}

	@XmlTransient
	public String getApplyOnSource() {
		return applyOnSource;
	}

	public void setApplyOnSource(String applyOnSource) {
		this.applyOnSource = applyOnSource;
	}

	@XmlTransient
	public String getGroupOn() {
		return groupOn;
	}

	public void setGroupOn(String groupOn) {
		this.groupOn = groupOn;
	}

	@XmlTransient
	public List<TripleCondition> getConditions() {
		return conditions;
	}

	public void setConditions(List<TripleCondition> conditions) {
		this.conditions = conditions;
	}

	public void appendCondition(TripleCondition condition) {
		getConditions().add(condition);
	}

}
