package ch.uzh.ddis.katts.query.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlID;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.katts.utils.XmlTypeMapping;

/**
 * A {@link Stream} has a set of variable. Each variable has a type, a name
 * and a reference name. The type indicates the processing nodes how to
 * handle the variable. The name is used to set a reference to this variable
 * in other structures. The reference to is to indicate the processing node 
 * for what the variable should be used. 
 * 
 * @author Thomas Hunziker
 *
 */
public class Variable implements Serializable{

	@XmlTransient
	private static final long serialVersionUID = 1L;

	@XmlTransient
	private String typeName;
	
	@SuppressWarnings("rawtypes")
	@XmlTransient
	private Class type;
	
	@XmlAttribute
	private String referencesTo;
	
	@XmlAttribute(required=true)
	@XmlID
	private String name = String.valueOf(Math.abs(Utils.secureRandomLong()));

	/**
	 * Returns the type as string. 
	 * 
	 * @return
	 */
	@XmlAttribute(required=true, name="type")
	public String getTypeName() {
		return typeName;
	}
	
	/**
	 * <p>This method returns the type of the variable as Java
	 * class type. It must be some basic XML type. Currently 
	 * the following types are supported:</p>
	 * <ul>
	 * 	<li>int</li>
	 *  <li>byte</li>
	 *  <li>double</li>
	 *  <li>float</li>
	 *  <li>long</li>
	 *  <li>boolean</li>
	 *  <li>string</li>
	 *  <li>integer</li>
	 *  <li>anyType (handles as {@link Object}</li>
	 *  <li>string</li>
	 *  <li>decimal</li>
	 *  <li>duration</li>
	 *  <li>date</li>
	 *  <li>dateTime</li>
	 *  <li>short</li>
	 *  <li>base64Binary</li>
	 *  <li>unsignedByte</li>
	 *  <li>unsignedInt</li>
	 *  <li>unsignedShort</li>
	 * </ul>
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	@XmlTransient
	public Class getType() {
		return this.type;
	}
	
	/**
	 * This sets the type of the variable. See {@link #getType()} for more information
	 * on the type and supported types.
	 * 
	 * @param type
	 */
	public void setTypeName(String type) {
		this.typeName = type;
		this.type = XmlTypeMapping.resolveXmlType(type);
	}

	/**
	 * Returns the reference to value. The reference to value
	 * is used by the node to identify what to do with
	 * the variable.
	 * 
	 * @return
	 */
	@XmlTransient
	public String getReferencesTo() {
		return referencesTo;
	}

	/**
	 * Sets the reference to value. 
	 *  
	 * @see #getReferencesTo()
	 * @param referenceTo
	 */
	public void setReferencesTo(String referenceTo) {
		this.referencesTo = referenceTo;
	}

	/**
	 * Returns the name of the variable. The name must be unique per 
	 * topology / query. The name identifies the variable in other
	 * components. If no name is set a random number sequence is returned. 
	 * 
	 * @return
	 */
	@XmlTransient
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the variable. 
	 * 
	 * @see #getName()
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	public static Fields getFieldsFromList(Collection<Variable> variables) {
		return new Fields(getFieldList(variables));
	}
	
	public static List<String> getFieldList(Collection<Variable> variables) {
		List<String> fields = new ArrayList<String>();
		
		for (Variable variable : variables) {
			fields.add(variable.getName());
		}
		
		return fields;
		
	}
	
	public int hashcode() {
		return this.getName().hashCode();
	}
	
	@Override
	public String toString() {
		return this.getName();
	}
	
}
