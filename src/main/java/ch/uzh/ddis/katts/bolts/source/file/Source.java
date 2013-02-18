package ch.uzh.ddis.katts.bolts.source.file;

import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

import ch.uzh.ddis.katts.bolts.source.FileTripleReader;
import ch.uzh.ddis.katts.query.source.File;

/**
 * This interface provides the basic functionalities required by a file based source. Such a source can be read in with
 * {@link FileTripleReader}.
 * 
 * A source can also used as a wrapper to combine different file like functionalities together to read in files.
 * 
 * @author Thomas Hunziker
 * 
 */
public interface Source extends Serializable {

	/**
	 * This method returns the InputStream from which the reader reads.
	 * 
	 * @param file
	 *            The location of the file to read.
	 * @return InputStream for reading in.
	 * @throws Exception
	 */
	public InputStream buildInputStream(File file) throws Exception;

	/**
	 * This method sets the InputStream created by the {@link Source#buildInputStream}. This allows wrappers to rewrite
	 * the InputStream.
	 * 
	 * @param inputStream
	 *            The InputStream to read from.
	 */
	public void setFileInputStream(InputStream inputStream);

	/**
	 * This method returns a triple given as a list of Strings.
	 * 
	 * @return Next triple in the file.
	 * @throws Exception
	 */
	public List<String> getNextTuple() throws Exception;

}
