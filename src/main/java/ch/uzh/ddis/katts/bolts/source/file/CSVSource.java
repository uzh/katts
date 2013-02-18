package ch.uzh.ddis.katts.bolts.source.file;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;
import ch.uzh.ddis.katts.query.source.File;

/**
 * This {@link Source} implementation reads from a CSV file. 
 * 
 * @author Thomas Hunziker
 *
 */
public class CSVSource implements Source{
	
	private static final long serialVersionUID = 1L;
	private transient CSVReader csvReader;
	private File file;
	
	@Override
	public List<String> getNextTuple() throws Exception {
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
		csvReader = new CSVReader(inputStreamReader, getDelimiter(), CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER, 0, true);
	}
	
	/**
	 * This method gets the delimiter (separator) for the CSV file. 
	 * 
	 * @return A delimiter char for the CSV file.
	 */
	public char getDelimiter() {
		return file.getCsvFieldDelimiter().charAt(0);
	}

}
