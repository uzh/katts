package ch.uzh.ddis.katts.query.processor.filter;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import backtype.storm.tuple.Tuple;

/**
 * This class represents the triple filter condition in XML. The condition
 * consists of the item (subject,predicate or object) and the condition that
 * this item must meet to be processed.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public interface NTupleCondition extends Serializable {

	boolean matches(Tuple input);

}
