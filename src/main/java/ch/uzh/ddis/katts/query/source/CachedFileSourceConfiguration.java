package ch.uzh.ddis.katts.query.source;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import backtype.storm.topology.IRichBolt;
import ch.uzh.ddis.katts.bolts.source.CachedFileTripleReader;
import ch.uzh.ddis.katts.bolts.source.FileTripleReaderConfiguration;

/**
 * The BachedFileSource does basically the same as the FileSource bolt with the following differences:
 * <ol>
 * <li>It reads the whole file into main memory at boot time and does not read from disk during the runs. This is useful
 * for preventing disk lag from influencing time measurements.</li>
 * <li>It supports parallelism by allowing the user to specify a blockSize variable. Each instance of the reader will
 * read #blockSize# number of lines before skipping #blockSize * parallelism# lines (other task instances read these
 * blocks). The offset (at which block each instance should start readin) will be computed during runtime.</li>
 * </ol>
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
@XmlRootElement(name="cachedFileSource")
@XmlAccessorType(XmlAccessType.FIELD)
public class CachedFileSourceConfiguration extends AbstractSource implements FileTripleReaderConfiguration {

	/**
	 * The number of tasks that should be instantiated for this bolt.
	 */
	@XmlAttribute(required = true)
	private int parallelism;

	/**
	 * The block size is the number of lines that each task instance should read, before skipping #parallelism *
	 * blockSize# lines, so other task instances can read these blocks.
	 */
	@XmlAttribute(required = true)
	private int blockSize;

	@XmlElementWrapper(name = "files")
	@XmlElement(name = "file")
	private List<File> files = new ArrayList<File>();

	@Override
	public IRichBolt getBolt() {
		CachedFileTripleReader reader = new CachedFileTripleReader();
		reader.setConfiguration(this);
		return reader;
	}
	
	@Override
	public boolean validate() {
		return true;
	}

	public List<File> getFiles() {
		return files;
	}

	public void setFiles(List<File> files) {
		this.files = files;
	}

	/**
	 * {@link CachedFileSourceConfiguration#parallelism}
	 * 
	 * @return the parallelism
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * {@link CachedFileSourceConfiguration#parallelism}
	 * 
	 * @param parallelism
	 *            the parallelism to set
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	/**
	 * {@link CachedFileSourceConfiguration#blockSize}
	 * 
	 * @return the blockSize
	 */
	public int getBlockSize() {
		return blockSize;
	}

	/**
	 * {@link CachedFileSourceConfiguration#blockSize}
	 * 
	 * @param blockSize
	 *            the blockSize to set
	 */
	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

}
