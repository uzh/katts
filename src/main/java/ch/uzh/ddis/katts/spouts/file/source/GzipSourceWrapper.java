package ch.uzh.ddis.katts.spouts.file.source;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.ZipException;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import ch.uzh.ddis.katts.query.source.File;

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
//		ZipArchiveEntry entry = zipFile.getEntry(file.getZipFileEntry());
//		return zipFile.getInputStream(entry);
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
