package ch.uzh.ddis.katts.query.processor.aggregate.component;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.aggregate.component.AvgPartitionerComponent;

/**
 * This class indicates that the aggregation function is configured as a max aggregation.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class AvgPartitioner extends AvgPartitionerComponent implements Serializable {

	private static final long serialVersionUID = 1L;

}
