package ch.uzh.ddis.katts.bolts.source.file;

import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import ch.uzh.ddis.katts.query.source.File;

/**
 * This class provides a wrapper around the sources to read in gziped files. Since the streams are used the
 * uncompression is done on the fly.
 * 
 * @author Thomas Hunziker
 * 
 */
public class PrefetchSourceWrapper implements Source {

	private LinkedList<List<String>> cache;

	private static final long serialVersionUID = 1L;

	/** We store the id of the source we're caching in this variable. */
	private String sourceId;
	
	/**
	 * Creates a PrefetchSourceWrapper that wrapps another source. We read out the content of this source into an
	 * internal (memory) datastructure and then return its content one-by-one through the {@link #getNextTuple()}
	 * method.
	 * 
	 * @param wrappedSource
	 *            the source that is being wrapped by this class.
	 * 
	 * @throws Exception
	 *             if there is an error while prefetching the file.
	 */
	public PrefetchSourceWrapper(Source wrappedSource, int blockSize, int numberOfTasks, int thisTaskId)
			throws Exception {
		this.sourceId = wrappedSource.getSourceId();		
		int currentLine = 0;

		this.cache = new LinkedList<List<String>>();

		while (true) {
			if (((currentLine / blockSize) % numberOfTasks) == thisTaskId) {
				List<String> nextTuple = wrappedSource.getNextTuple();
				cache.add(nextTuple);
				if (nextTuple == null) {
					break; // exit the loop
				}
			}

			currentLine++;
		}
	}

	@Override
	public InputStream buildInputStream(File file) throws Exception {
		throw new IllegalStateException("there is no reason to call this method");
	}

	@Override
	public void setFileInputStream(InputStream inputStream) {
		throw new IllegalStateException("there is no reason to call this method");
	}

	@Override
	public List<String> getNextTuple() throws Exception {
		// returning next tuple from cache
		return this.cache.poll();
	}
	
	@Override
	public String getSourceId() {
		return this.sourceId;
	}

}
