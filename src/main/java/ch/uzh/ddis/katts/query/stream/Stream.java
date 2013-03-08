package ch.uzh.ddis.katts.query.stream;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlID;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.query.Node;

/**
 * The single nodes in the topology are connected with streams. This class represents the configuration of such a
 * stream. A stream consists always of a set of variables and a source. The consuming part of the stream is configured
 * in {@link StreamConsumer}.
 * 
 * @see Variable
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class Stream implements Serializable {

	private static final long serialVersionUID = 1L;

	@XmlTransient
	private String id = String.valueOf(Math.abs(Utils.secureRandomLong()));

	@XmlTransient
	private VariableList variables = new VariableList();

	@XmlAttribute(name="output")
	private boolean outputFlag = false;

	@XmlTransient
	private Stream inheritFrom = null;

	@XmlTransient
	private Node node;

	@XmlTransient
	private Duration eventTimeOverlapping = null;

	public Stream() throws DatatypeConfigurationException {
		if (getEventTimeOverlapping() == null) {
			setEventTimeOverlapping(DatatypeFactory.newInstance().newDuration(0));
		}
	}

	@XmlID
	@XmlAttribute(required = true)
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@XmlElement(name = "variable")
	public VariableList getVariables() {
		return variables;
	}

	/**
	 * This method returns all variables including the inherit ones.
	 * 
	 * @return
	 */
	@XmlTransient
	public VariableList getAllVariables() {
		if (this.getInheritFrom() != null) {
			// TODO: Hash somehow the combined variable list
			VariableList vars = new VariableList();
			vars.addAll(this.getInheritFrom().getAllVariables());
			vars.addAll(this.variables);
			return vars;
		} else {
			return variables;
		}
	}

	public void setVariables(List<Variable> variables) {
		this.variables.clear();
		this.variables.addAll(variables);
	}

	public void appendVariable(Variable variable) {
		this.variables.add(variable);
	}

	public Variable getVariableByReferenceName(String referenceName) {
		return this.variables.getVariableReferencesTo(referenceName);
	}

	@XmlIDREF
	@XmlAttribute(name = "inheritFrom")
	public Stream getInheritFrom() {
		return inheritFrom;
	}

	public void setInheritFrom(Stream inheritFrom) {
		this.inheritFrom = inheritFrom;
	}

	@XmlTransient
	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}

	//
	// public boolean hasVariable(Variable variable) {
	// return this.getAllVariables().contains(variable);
	// }

	// @XmlAttribute(name="eventTimeOverlapping", required=false)
	@XmlTransient
	public Duration getEventTimeOverlapping() {
		return eventTimeOverlapping;
	}

	public void setEventTimeOverlapping(Duration eventTimeOverlapping) {
		this.eventTimeOverlapping = eventTimeOverlapping;
	}

	@Override
	public int hashCode() {
		return this.getId().hashCode();
	}

	/**
	 * @return the outputStream
	 */
	public boolean isOutputFlag() {
		return this.outputFlag;
	}

	/**
	 * @param outputStream the outputStream to set
	 */
	public void setOutputFlag(boolean outputFlag) {
		this.outputFlag = outputFlag;
	}

}
