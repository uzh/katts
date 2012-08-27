package ch.uzh.ddis.katts.spouts.file;

import java.util.List;

import ch.uzh.ddis.katts.bolts.ProducerConfiguration;
import ch.uzh.ddis.katts.query.source.File;

public interface FileTripleReaderConfiguration{
	
	/**
	 * This method returns a set of files to load.
	 * 
	 * @return
	 */
	public List<File> getFiles();
	
	public String getId();

}
