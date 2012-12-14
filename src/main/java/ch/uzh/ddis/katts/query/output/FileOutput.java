package ch.uzh.ddis.katts.query.output;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.output.FileOutputBolt;
import ch.uzh.ddis.katts.bolts.output.FileOutputConfiguration;

/**
 * This class implements the XML configuration node for a file output Bolt.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FileOutput extends AbstractOutput implements FileOutputConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlTransient
	private String filePath;

	@Override
	@XmlTransient
	public Bolt getBolt() {
		FileOutputBolt bolt = new FileOutputBolt();
		bolt.setConfiguration(this);
		return bolt;
	}

	@Override
	@XmlAttribute(required = true)
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

}
