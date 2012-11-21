package ch.uzh.ddis.katts.bolts.source.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.zip.ZipException;

import ch.uzh.ddis.katts.query.source.File;

public interface Source extends Serializable{
	
	public InputStream buildInputStream(File file) throws Exception;
	
	public void setFileInputStream(InputStream inputStream);
	
	public List<String> getNextTriple() throws Exception;
	
}
