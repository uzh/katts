package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Tuple;

/**
 * 
 * Instances of this class hold key value pairs. All keys are required to be of type string, while the values can be of
 * any type.
 * 
 * @author Lorenz Fischer
 * 
 *         TODO: Replace this class with {@link ch.uzh.ddis.katts.bolts.Event}.
 * 
 */
public class SimpleVariableBindings extends HashMap<String, Object> {

	public static String FIELD_STARTDATE = "startDate";
	public static String FIELD_ENDDATE = "endDate";

	private static final long serialVersionUID = 1L;

	public SimpleVariableBindings() {
	}

	public SimpleVariableBindings(Tuple tuple) {
		for (String variableName : tuple.getFields()) {
			put(variableName, tuple.getValueByField(variableName));
		}
	}

	public Long getStartDate() {
		return ((Date) get(FIELD_STARTDATE)).getTime();
	}

	public Long getEndDate() {
		return ((Date) get(FIELD_ENDDATE)).getTime();
	}

	@Override
	public String toString() {
		// create the string in ascending order of of the keys

		StringBuilder result = new StringBuilder();
		List<String> sortedKeys = new ArrayList<String>(keySet());

		Collections.sort(sortedKeys);

		for (String key : sortedKeys) {
			if (result.length() > 0) {
				result.append(",");
			}
			result.append(key).append("=").append(get(key).toString());
		}

		return result.toString();
	}

}
