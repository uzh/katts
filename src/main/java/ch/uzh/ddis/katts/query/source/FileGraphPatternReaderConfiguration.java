package ch.uzh.ddis.katts.query.source;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import backtype.storm.topology.IRichSpout;
import ch.uzh.ddis.katts.bolts.source.FileGraphPatternReader;
import ch.uzh.ddis.katts.bolts.source.FileTripleReaderConfiguration;

/**
 * The FileGraphPatternReader reads files that contain time annotated triples. The configured graph pattern will be
 * matched against groups of these triples. All triples that share the same timestamp are treated as belonging to the
 * same group. The FileGraphPatternReader can be configured using the same file types as the regular FileSource-node
 * (csv, zip, gzipped etc).
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
@XmlRootElement(name = "FileGraphPatternReader")
@XmlAccessorType(XmlAccessType.FIELD)
public class FileGraphPatternReaderConfiguration extends AbstractSource implements FileTripleReaderConfiguration {

	/** The graph SPARQL graph pattern to match against the triples. */
	@XmlAttribute(name = "pattern")
	private String graphPattern;

	@Override
	public IRichSpout getSpout() {
		return new FileGraphPatternReader(this);
	}

}
