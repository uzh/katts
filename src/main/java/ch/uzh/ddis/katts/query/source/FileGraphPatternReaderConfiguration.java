package ch.uzh.ddis.katts.query.source;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.IRichSpout;
import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.bolts.source.FileGraphPatternReader;
import ch.uzh.ddis.katts.bolts.source.FileTripleReaderConfiguration;
import ch.uzh.ddis.katts.query.stream.Producers;
import ch.uzh.ddis.katts.query.stream.Stream;

/**
 * The FileGraphPatternReader reads files that contain time annotated triples. The configured graph pattern will be
 * matched against groups of these triples. All triples that share the same timestamp are treated as belonging to the
 * same group. The FileGraphPatternReader can be configured using the same file types as the regular FileSource-node
 * (csv, zip, gzipped etc).
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
@XmlRootElement(name = "fileGraphPatternReader")
@XmlAccessorType(XmlAccessType.FIELD)
public class FileGraphPatternReaderConfiguration extends AbstractSource implements FileTripleReaderConfiguration, ProducerConfiguration {

	/**
	 * This is a list of triple patterns that have to be matched agains the triple stream.
	 */
	@XmlElementWrapper(name = "patterns")
	@XmlElement(name = "pattern")
	private List<String> patterns = new ArrayList<String>();
	
	// TODO lorenz: remove the variables construct, we only need this because of this referential integrity thing..
	@XmlElementWrapper(name = "produces")
	@XmlElement(name = "stream")
	private List<Stream> producers = new Producers(this);
	
	public List<String> getPatterns() {
		return patterns;
	}

	public void setPatterns(List<String> patterns) {
		this.patterns = patterns;
	}

	@Override
	public IRichSpout getSpout() {
		return new FileGraphPatternReader(this);
	}

	public void setProducers(List<Stream> producers) {
		this.producers = producers;
	}
	
	@Override
	public List<Stream> getProducers() {
		return producers;
	}

}
