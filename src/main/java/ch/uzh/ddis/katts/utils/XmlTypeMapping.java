package ch.uzh.ddis.katts.utils;

import java.lang.reflect.InvocationTargetException;

/**
 * This class helps to map between XML types and java class types.
 * 
 * See http://docs.oracle.com/cd/E12840_01/wls/docs103/webserv/data_types.html#wp223908
 * 
 * @author Thomas Hunziker
 * 
 */
public final class XmlTypeMapping {

	@SuppressWarnings("unchecked")
	public final static <T> T converFromString(String input, Class<?> type) throws SecurityException,
			NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {

		if (type.equals(double.class)) {
			if (input.isEmpty()) {
				return (T) new Double(0);
			} else {
				return (T) Double.valueOf(input);
			}
		}

		return (T) input;
	}

	@SuppressWarnings("rawtypes")
	public final static Class resolveXmlType(String typeWithNameSpace) {

		// Remove potential name spaces
		String type = typeWithNameSpace.replace("xsd:", "").replace("xs:", "");

		if (type.equals("boolean")) {
			return boolean.class;
		} else if (type.equals("byte")) {
			return byte.class;
		} else if (type.equals("double")) {
			return double.class;
		} else if (type.equals("float")) {
			return float.class;
		} else if (type.equals("long")) {
			return long.class;
		} else if (type.equals("int")) {
			return int.class;
		} else if (type.equals("anyType")) {
			return java.lang.Object.class;
		} else if (type.equals("string")) {
			return java.lang.String.class;
		} else if (type.equals("integer")) {
			return java.math.BigInteger.class;
		} else if (type.equals("decimal")) {
			return java.math.BigDecimal.class;
		} else if (type.equals("duration")) {
			return javax.xml.datatype.Duration.class;
		} else if (type.equals("date")) {
			return javax.xml.datatype.XMLGregorianCalendar.class;
		} else if (type.equals("dateTime")) {
			return java.util.Calendar.class;
		} else if (type.equals("g")) {
			return javax.xml.datatype.XMLGregorianCalendar.class;
		} else if (type.equals("time")) {
			return javax.xml.datatype.XMLGregorianCalendar.class;
		} else if (type.equals("short")) {
			return short.class;
		} else if (type.equals("base64Binary")) {
			return byte[].class;
		} else if (type.equals("hexBinary")) {
			return byte[].class;
		} else if (type.equals("NOTATION")) {
			return javax.xml.namespace.QName.class;
		} else if (type.equals("Qname")) {
			return javax.xml.namespace.QName.class;
		} else if (type.equals("unsignedByte")) {
			return short.class;
		} else if (type.equals("unsignedInt")) {
			return long.class;
		} else if (type.equals("unsignedShort")) {
			return int.class;
		} else {
			throw new IllegalArgumentException("The given XML data type '" + type + "' is not supported.");
		}
	}

}
