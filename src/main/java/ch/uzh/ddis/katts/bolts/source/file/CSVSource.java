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
	

	/** If > 0, only lines up to this number (exclusive, 0-based) will be read. */
	private long readToLineNo = 0;

	/** We use this to keep track of what line (int the file) we're currently reading from. */
	private long currentLine = 0;
	
	/**
	 * Creates a source that only reads the supplied file up until line <code>readToLine</code>.
	 * @param readToLineNo the line number (exclusive) up to which the reader will read the input file. If this value
	 * is 0, no limit will be set.
	 */
	public CSVSource(long readToLineNo) {
		this.readToLineNo = readToLineNo;
	}
	
	@Override
	public List<String> getNextTuple() throws Exception {
		List<String> result = null;
		
		if (this.readToLineNo == 0 || this.currentLine++ < this.readToLineNo) {
			String[] line = null;
			line  = csvReader.readNext();
			
			if (line != null) {
				result = Arrays.asList(line);
			}
		}
		
		return result;
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

	@Override
	public String getSourceId() {
		return this.file.getPath();
	}

}
