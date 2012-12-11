package ch.uzh.ddis.katts.bolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;

/**
 * The variable bindings are used to exchange data between the different nodes (bolts). A object of variable bindings
 * contains a set of variable and always a start and end date. Additionally also a sequence number.
 * 
 * This sequence number is unique between two instance of a certain bolt. If a bolt is parallelized then the sequence
 * number can help to synchronize certain tasks. Important is that this sequence number is not globally nor unique
 * between two bolt types (classes). It is only unique between a direct stream from one bolt instance to the other. That
 * means it is unique on direct connections of bolts.
 * 
 * @author Thomas Hunziker
 * 
 */
public class VariableBindings {

	/** The stream on which the variable bindings of this object should be emitted on. */
	private Stream stream;
	private Emitter emitter = null;
	private Map<Variable, Object> variableData = new HashMap<Variable, Object>();
	private Event anchorEvent;
	private Date startDate;
	private Date endDate;

	public VariableBindings(Stream stream, Emitter emitter, Event anchorEvent) {
		this.stream = stream;
		this.emitter = emitter;
		this.anchorEvent = anchorEvent;
		this.startDate = this.anchorEvent.getStartDate();
		this.endDate = this.anchorEvent.getEndDate();
	}

	/**
	 * Adds a variable to the variable binding.
	 * 
	 * @param variable
	 *            The variable to add.
	 * @param value
	 *            The value for this variable in the variable binding.
	 * 
	 */
	public void add(Variable variable, Object value) {
		variableData.put(variable, value);
	}

	/**
	 * Adds a new variable to the variable binding by the given reference on identifier. The reference name identifies
	 * the internal name of a variable.
	 * 
	 * @param referenceName
	 *            The reference name for which the variable value should be set for.
	 * @param value
	 *            The value to set.
	 */
	public void add(String referenceName, Object value) {
		this.add(stream.getVariableByReferenceName(referenceName), value);
	}

	public Stream getStream() {
		return stream;
	}

	public void setStream(Stream stream) {
		this.stream = stream;
	}

	public void emit() {
		emitter.emit(this);
	}

	/**
	 * Build the outgoing tuple for storm with the correct ordering of values.
	 * 
	 * @param sequenceNumber
	 *            The sequence number of the variable binding.
	 * @return Tuple for emitting to Storm
	 */
	public List<Object> getDataListSorted(long sequenceNumber) {
		List<Object> list = new ArrayList<Object>();
		list.add(sequenceNumber);
		list.add(this.getStartDate());
		list.add(this.getEndDate());

		for (Variable var : this.getStream().getAllVariables()) {
			list.add(this.variableData.get(var));
		}
		return list;
	}

	/**
	 * The start date indicates, when the variable binding starts.
	 * 
	 * @return
	 */
	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	/**
	 * The end date indicates, when the variable binding ends.
	 * 
	 * @return
	 */
	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Event getAnchorEvent() {
		return anchorEvent;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("Emit On: ").append(stream.getId());
		builder.append("\n\tVariables:\n");

		for (Entry<Variable, Object> entry : this.variableData.entrySet()) {
			builder.append("\t");
			builder.append(entry.getKey());
			// builder.append("(").append(entry.getKey().getReferencesTo()).append(")");
			builder.append(": ");
			builder.append(entry.getValue());
			builder.append("\n");
		}

		return builder.toString();
	}

}
