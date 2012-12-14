package ch.uzh.ddis.katts.query.stream.grouping;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * This {@link Grouping} sends the variable bindings always to the same group. The group can be build up on different
 * variable values. The {@link GroupOn} indicates on which {@link Variable} value(s) should be grouped on.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class VariableGrouping implements Grouping {

	private static final long serialVersionUID = 1L;

	@XmlElementRefs({ @XmlElementRef(type = GroupOn.class), })
	private List<GroupOn> groupOnVariables = new ArrayList<GroupOn>();

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public void attachToBolt(BoltDeclarer bolt, StreamConsumer stream) {
		List<String> variables = new ArrayList<String>();

		for (GroupOn groupOn : this.getGroupOnVariables()) {
			variables.add(groupOn.getVariable().getName());
		}

		bolt.fieldsGrouping(stream.getStream().getNode().getId(), stream.getStream().getId(), new Fields(variables));
	}

	/**
	 * The {@link GroupOn} defines on which {@link Variable} value should the groups be build. Each group is then
	 * processed on the same machine and in the same thread. This allows aggregation on certain values.
	 * 
	 * @return
	 */
	@XmlTransient
	public List<GroupOn> getGroupOnVariables() {
		return groupOnVariables;
	}

	/**
	 * Sets the {@link GroupOn} values.
	 * 
	 * @see #getGroupOnVariables()
	 * @param groupOnVariables
	 */
	public void setGroupOnVariables(List<GroupOn> groupOnVariables) {
		this.groupOnVariables = groupOnVariables;
	}

	/**
	 * Adds a {@link GroupOn} value.
	 * 
	 * @see #getGroupOnVariables()
	 * @param groupOnVariables
	 */
	public void appendGroupOnVariable(Variable variable) {
		this.getGroupOnVariables().add(new GroupOn(variable));
	}

}
