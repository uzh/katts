package ch.uzh.ddis.katts.query.stream.grouping;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlIDREF;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This is class is used to setup groups on {@link Variable} values.
 * 
 * @see VariableGrouping
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GroupOn implements Serializable {

	private static final long serialVersionUID = 1L;

	public GroupOn() {

	}

	public GroupOn(Variable variable) {
		this.setVariable(variable);
	}

	@XmlIDREF
	@XmlAttribute(required = true)
	private Variable variableName;

	/**
	 * Returns the {@link Variable} on which the group should be created.
	 * 
	 * @return
	 */
	@XmlTransient
	public Variable getVariable() {
		return variableName;
	}

	/**
	 * Returns the {@link Variable} on which the group is built.
	 * 
	 * @param variableName
	 */
	public void setVariable(Variable variableName) {
		this.variableName = variableName;
	}
}
