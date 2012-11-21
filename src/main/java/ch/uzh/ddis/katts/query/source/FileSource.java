package ch.uzh.ddis.katts.query.source;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import ch.uzh.ddis.katts.bolts.source.FileTripleReader;
import ch.uzh.ddis.katts.bolts.source.FileTripleReaderConfiguration;

/**
 * The file source node is a source node which constructs from files data streams.
 * Where the data is emitted as triples.
 * 
 * A file source can handle multiple files and synchronize them depending on the
 * triple time.
 * 
 * @author Thomas Hunziker
 *
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FileSource extends AbstractSource implements FileTripleReaderConfiguration{
	
	private static final long serialVersionUID = 1L;
	
	@XmlElementWrapper(name="files")
	@XmlElement(name="file")
	private List<File> files = new ArrayList<File>();
	

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public IRichBolt getBolt() {
		FileTripleReader reader = new FileTripleReader();
		reader.setConfiguration(this);
		return reader;
	}

	@XmlTransient
	public List<File> getFiles() {
		return files;
	}

	public void setFiles(List<File> files) {
		this.files = files;
	}
	
	public void appendFile(File file) {
		this.getFiles().add(file);
	}

}
