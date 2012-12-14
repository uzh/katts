package ch.uzh.ddis.katts.bolts.source.file;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import ch.uzh.ddis.katts.query.source.File;

/**
 * This class provides a wrapper around the sources to read in gziped files. Since the streams are used the
 * uncompression is done on the fly.
 * 
 * @author Thomas Hunziker
 * 
 */
public class GzipSourceWrapper implements Source {

	private static final long serialVersionUID = 1L;
	private Source component;

	public GzipSourceWrapper(Source component) {
		this.component = component;
	}

	@Override
	public InputStream buildInputStream(File file) throws Exception {
		component.buildInputStream(file);
		return new GzipCompressorInputStream(new FileInputStream(file.getPath()));
	}

	@Override
	public void setFileInputStream(InputStream inputStream) {
		component.setFileInputStream(inputStream);
	}

	@Override
	public List<String> getNextTriple() throws Exception {
		return component.getNextTriple();
	}

}
