package ch.uzh.ddis.katts.query.processor.filter;

import javax.xml.bind.annotation.XmlEnumValue;

/**
 * This enum contains the different operators supported by the triple filter {@link TripleFilter}.
 * 
 * @author Thomas Hunziker
 * 
 */
public enum Operator {

	@XmlEnumValue("NE")
	NE("NotEqual"),

	@XmlEnumValue("EQ")
	EQ("Equal"),

	@XmlEnumValue("EQ")
	LT("LessThan"),

	@XmlEnumValue("EQ")
	LE("LessOrEqualThan"),

	@XmlEnumValue("GT")
	GT("GreaterThan"),

	@XmlEnumValue("GE")
	GE("GreaterOrEqualThan");

	private final String value;

	Operator(String v) {
		value = v;
	}

	public String value() {
		return value;
	}

	public static Operator fromValue(String v) {
		for (Operator c : Operator.values()) {
			if (c.value.equals(v)) {
				return c;
			}
		}
		throw new IllegalArgumentException(v);
	}

}
