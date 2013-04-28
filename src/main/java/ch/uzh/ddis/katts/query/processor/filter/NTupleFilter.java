package ch.uzh.ddis.katts.query.processor.filter;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.bolts.filter.NTupleFilterBolt;
import ch.uzh.ddis.katts.bolts.filter.NTupleFilterConfiguration;
import ch.uzh.ddis.katts.query.AbstractNode;
import ch.uzh.ddis.katts.query.ProducerNode;
import ch.uzh.ddis.katts.query.source.FileSource;
import ch.uzh.ddis.katts.query.source.Source;
import ch.uzh.ddis.katts.query.stream.Producers;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.validation.InvalidNodeConfigurationException;

/**
 * Triple filters convert a stream of time annotated triples (quadruples) into a stream of variable bindings. Each
 * triple filter is responsible for only one triple pattern. The patterns can be configured using a set of
 * {@link TripleCondition} objects. Each condition object specifies the content of one of the three fields subject,
 * predicate, or object. The values in the tiples are compared to the values of the condition using the equals operator.
 * 
 * Triple filters are always applied to a specific {@link Source} nodes such as the {@link FileSource}.
 * 
 * The output stream of a triple filter is a stream of variable bindings (i.e. a stream of lists of key-value pairs).
 * 
 * @author Thomas Hunziker
 */
@XmlRootElement
public class NTupleFilter extends AbstractNode implements ProducerNode, NTupleFilterConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlAttribute(name = "applyOnSource")
	private String applyOnSource;

	@XmlAttribute(name = "groupOn")
	private String groupOn;

	@XmlElementWrapper(name = "conditions")
	@XmlAnyElement
	@XmlElementRefs({ @XmlElementRef(type = ConstantNTupleCondition.class),
			@XmlElementRef(type = VariableNTupleCondition.class) })
	private List<NTupleCondition> conditions = new ArrayList<NTupleCondition>();

	@XmlTransient
	private List<Stream> producers = new Producers(this);

	public NTupleFilter() {
		return;
	}

	@XmlElementWrapper(name = "produces")
	@XmlElement(name = "stream")
	@Override
	public List<Stream> getProducers() {
		return producers;
	}

	public void setProducers(List<Stream> producers) {
		this.producers = producers;
	}

	public void appendProducer(Stream producer) {
		this.getProducers().add(producer);
	}

	@Override
	public void createTopology(TopologyBuilder builder) {
		NTupleFilterBolt bolt = new NTupleFilterBolt();
		bolt.setConfiguration(this);
		BoltDeclarer declarer = builder.setBolt(this.getId(), bolt, getParallelism());
		if (groupOn == null || groupOn.isEmpty()) {
			declarer.localOrShuffleGrouping(applyOnSource);
		} else {
			declarer.fieldsGrouping(applyOnSource, new Fields(groupOn));
		}
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

	/**
	 * The group on is optional. If no group on is defined a local or shuffle grouping is used to distribute the
	 * triples. The group on indicates how to group the incoming stream from the triple source.
	 * 
	 * @return
	 */
	@XmlTransient
	public String getGroupOn() {
		return groupOn;
	}

	public void setGroupOn(String groupOn) {
		this.groupOn = groupOn;
	}

	@XmlTransient
	public List<NTupleCondition> getConditions() {
		return conditions;
	}

	public void setConditions(List<NTupleCondition> conditions) {
		this.conditions = conditions;
	}

	public void appendCondition(NTupleCondition condition) {
		getConditions().add(condition);
	}

}
