package ch.uzh.ddis.katts.bolts.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;

/**
 * @Thomas: This is a temporary class. It will most likely be removed as soon as I
 * understand the differences between Event, Tuple and VariableBindings. Most
 * probably I should be using one of these instead of this class.
 * 
 * Instances of this class hold key value pairs. All keys are required to be of
 * type string, while the values can be of any type.
 * 
 * @author fischer
 * 
 */
@SuppressWarnings("rawtypes")
// tuple is not typed, so we can't really do anything here.
public class SimpleVariableBindings extends HashMap<String, Object> {

	public SimpleVariableBindings() {
	}

	public SimpleVariableBindings(Tuple tuple) {
		for (Object item : tuple.entrySet()) {
			Map.Entry entry = (Map.Entry) item;
			
			put(entry.getKey().toString(), entry.getValue());
		}
	}

	public Long getStartDate() {
		return ((Date) get("startDate")).getTime();
	}

	public Long getEndDate() {
		return ((Date) get("endDate")).getTime();
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
