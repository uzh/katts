package ch.uzh.ddis.katts.bolts.source;

import java.util.List;

import ch.uzh.ddis.katts.query.source.File;

// TODO lorenz: maybe rename this into a general FileReaderConfiguration?
/**
 * This interface provides the basic configuration required by {@link FileTripleReader}.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface FileTripleReaderConfiguration {

	/**
	 * This method returns a set of files to load.
	 * 
	 * @return List of files to read in.
	 */
	public List<File> getFiles();

	/**
	 * This method returns the component id of the reader. This does also identify the source.
	 * 
	 * @return Source id and component id
	 */
	public String getId();

}
