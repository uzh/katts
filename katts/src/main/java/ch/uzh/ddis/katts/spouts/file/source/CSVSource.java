package ch.uzh.ddis.katts.spouts.file.source;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import ch.uzh.ddis.katts.query.source.File;

public class CSVSource extends AbstractSource {

	private transient CSVReader csvReader;
	private File file;

	@Override
	public List<String> getNextTriple() throws Exception {
		String[] line = csvReader.readNext();
		if (line == null) {
			return null;
		}
		return Arrays.asList(line);
	}

	@Override
	public InputStream buildInputStream(File file) throws FileNotFoundException {
		this.file = file;
		return new FileInputStream(file.getPath());
	}

	@Override
	public void setFileInputStream(InputStream inputStream) {
		Reader inputStreamReader = new InputStreamReader(inputStream);
		csvReader = new CSVReader(inputStreamReader, getDelimiter());
	}

	public char getDelimiter() {
		return file.getCsvFieldDelimiter().charAt(0);
	}

}
