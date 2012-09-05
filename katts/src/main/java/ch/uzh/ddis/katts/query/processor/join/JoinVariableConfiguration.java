package ch.uzh.ddis.katts.query.processor.join;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class is used by join condition configuration objects that need multiple variable definitions to join over. One
 * example of this is the {@link RegularJoinConditionConfiguration} class.
 * 
 * @author fischer
 * @see RegularJoinConditionConfiguration
 */
@XmlRootElement(name = "joinVariable")
@XmlAccessorType(XmlAccessType.FIELD)
public class JoinVariableConfiguration implements Serializable {

	/**
	 * The name of the field, whose value should be used for the join.
	 */
	@XmlAttribute(name = "fieldName", required = true)
	private String joinField;

	/**
	 * The identifier of the stream on which the value of the {@link #joinField} should be read from.
	 */
	@XmlAttribute(name = "streamId", required = true)
	private String streamId;

	public void setJoinField(String joinField) {
		this.joinField = joinField;
	}

	public String getJoinField() {
		return joinField;
	}

	public String getStreamId() {
		return streamId;
	}

	public void setStreamId(String streamId) {
		this.streamId = streamId;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();

		result.append("<");
		result.append(getClass().getAnnotation(XmlRootElement.class).name());
		try {
			result.append(getClass().getField("joinField").getAnnotation(XmlAttribute.class).name()).append("='")
					.append(joinField).append("' ");
			result.append(getClass().getField("streamId").getAnnotation(XmlAttribute.class).name()).append("='")
					.append(streamId).append("' ");
		} catch (Exception e) {
			throw new RuntimeException("Couldn't create toString() value for " + getClass().getName(), e);
		}
		result.append("/>");

		return result.toString();
	}
}
