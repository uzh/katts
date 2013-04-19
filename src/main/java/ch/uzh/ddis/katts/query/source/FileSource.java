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
 * The FileTripleReader reads CSV-Files containing time annotated triples (quadruples). Each quadruple needs to be on a
 * new line. The fields need to be in the following order: Date, Subject, Predicate, Object. Dates can be specified
 * either as a number depicting the number of milliseconds since 1/1/1970 or be in the ISO standard date format as
 * implemented by org.joda.time.format.ISODateTimeFormat class. The reader supports plaintext, zipped, or gzipped files.
 * For ZIP files, the user needs to specify the name of the file in the archive. If multiple files are read in
 * concurrently, one instance of the file reader bolt is created per file.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class FileSource extends AbstractSource implements FileTripleReaderConfiguration {

	private static final long serialVersionUID = 1L;

	@XmlElementWrapper(name = "files")
	@XmlElement(name = "file")
	private List<File> files = new ArrayList<File>();

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public IRichSpout getSpout() {
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

	@Override
	@XmlTransient
	public int getParallelism() {
		// We need as many bolts, as we have files in our list.
		return this.files.size();
	}
}
