package ch.uzh.ddis.katts.query.output;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.output.FileOutputBolt;
import ch.uzh.ddis.katts.bolts.output.FileOutputConfiguration;

/**
 * The FileOutput-Bolt writes the contents of an incoming stream into a CSV-file. Each line is one list of key-value
 * pairs. The keys are listed on the first line. The value lines don't contain the names of the field, but only the
 * values of the respective field.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FileOutput extends AbstractOutput implements FileOutputConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlAttribute(required = true)
	private String filePath;

	@Override
	public Bolt createBoltInstance() {
		FileOutputBolt bolt = new FileOutputBolt();
		bolt.setConfiguration(this);
		return bolt;
	}

	@Override
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

}
