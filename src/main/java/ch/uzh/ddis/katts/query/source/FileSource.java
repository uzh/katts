package ch.uzh.ddis.katts.query.source;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

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

	@Override
	public IRichSpout getSpout() {
		return new FileTripleReader(this);
	}

}
