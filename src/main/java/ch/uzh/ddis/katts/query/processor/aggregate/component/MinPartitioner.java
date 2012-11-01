package ch.uzh.ddis.katts.query.processor.aggregate.component;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.aggregate.component.MinPartitionerComponent;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class MinPartitioner extends MinPartitionerComponent implements Serializable{

	private static final long serialVersionUID = 1L;

}
