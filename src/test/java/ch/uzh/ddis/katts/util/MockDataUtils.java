package ch.uzh.ddis.katts.util;

import java.text.ParseException;
import java.util.regex.Pattern;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;

/**
 * Utility class containing static methods useful for the creation of mock data inside the unit tests.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public final class MockDataUtils {

	public final static DateTimeFormatter ISO_FORMAT = ISODateTimeFormat.dateTimeParser();

	/**
	 * Parses a comma separated list of key-value pairs and writes them into a new instance of SimpleVariableBindings.
	 * 
	 * The two fields "startDate" and "endDate" need to be formatted in the ISO date format as specified by
	 * {@link DateTimeFormatter}. If only startDate is provided, the same value will be used for the "endDate" field as
	 * well.
	 * 
	 * @param stringToParse
	 *            the string in the form "key1=value1,key2=value2". This implementation does not support escaping.
	 * @throws ParseException
	 *             if stringToParse could not be parsed.
	 */
	public static SimpleVariableBindings parseString(String stringToParse) throws ParseException {

		Pattern longPattern = Pattern.compile("^[0-9]*$");
		Pattern doublePattern = Pattern.compile("[0-9]*.[0-9]*D");
		SimpleVariableBindings result = new SimpleVariableBindings();

		for (String keyValuePair : stringToParse.split(",")) {
			String[] kvArr = keyValuePair.split("=");
			try {
				String key = kvArr[0];
				String value = kvArr[1];
				if (SimpleVariableBindings.FIELD_STARTDATE.equals(key)
						|| SimpleVariableBindings.FIELD_ENDDATE.equals(key)) {
					result.put(key, ISO_FORMAT.parseDateTime(value).toDate());
				} else {
					if (doublePattern.matcher(value).matches()) {
						result.put(key, Double.valueOf(value));
					} else if (longPattern.matcher(value).matches()) {
						result.put(key, Long.valueOf(value));
					} else {
						result.put(key, value);
					}
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				throw new ParseException("Could not parse string " + keyValuePair, 0);
			}
		}

		if (result.containsKey(SimpleVariableBindings.FIELD_STARTDATE)
				&& !result.containsKey(SimpleVariableBindings.FIELD_ENDDATE)) {
			result.put(SimpleVariableBindings.FIELD_ENDDATE, result.get(SimpleVariableBindings.FIELD_STARTDATE));
		}

		return result;
	}

	/**
	 * Creates a values object using the fieldnames of <code>fields</code> and the values inside <code>bindings</code>.
	 * 
	 * @param fields
	 *            the names of the fields whose values should be put into the resulting values object.
	 * @param bindings
	 *            the variable bindings to use for the values object.
	 * @return the values object.
	 */
	public static Values convertToValues(Fields fields, SimpleVariableBindings bindings) {
		Values result = new Values();

		for (String fieldName : fields.toList()) {
			result.add(bindings.get(fieldName));
		}

		return result;
	}

}
