package ch.uzh.ddis.katts.query.processor.filter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.tuple.Tuple;

/**
 * This condition is used to check whether a list of fields of an n-tuple have
 * the same value. In terms of a graph pattern, this condition applies a join on
 * a list of fields of the same n-tuple.
 * 
 * @author Thomas Scharrenbach
 * 
 */
@XmlRootElement(name = "variableCondition")
@XmlAccessorType(XmlAccessType.FIELD)
public class VariableNTupleCondition implements NTupleCondition {

	private static final long serialVersionUID = 1L;

	public VariableNTupleCondition() {
	}

	@Override
	public boolean matches(Tuple input) {

		// Joins need to have at least two elements. They are a tautology, else.
		if (getFields().size() < 2) {
			return true;
		}
		Iterator<String> it = fields.iterator();
		String var = it.next();
		String value = input.getStringByField(var);

		// Compare all specified fields
		while (it.hasNext()) {
			var = it.next();
			if (!value.equals(input.getStringByField(var))) {
				return false;
			}
		}
		// If not yet returned, then all join fields have the same value.
		return true;
	}

	@XmlElement(name = "field")
	private List<String> fields = new ArrayList<String>();

	@XmlTransient
	public List<String> getFields() {
		return this.fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

}
