package ch.uzh.ddis.katts.bolts.source.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import ch.uzh.ddis.katts.query.source.File;

/**
 * This {@link Source} reads files that have the following format
 * 
 * unixTimeStamp unixTimeStamp subject predicate object .
 * 
 * This reader removes quotation marks.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class N5Source implements Source {

	/** We use this reader to read the file. */
	private BufferedReader reader;

	/** This id will contain the path of the file we're reading. */
	private String sourceId;
	
	/** If > 0, only lines up to this number (exclusive, 0-based) will be read. */
	private long readToLineNo = 0;

	/** We use this to keep track of what line (int the file) we're currently reading from. */
	private long currentLine = 0;
	
	/**
	 * Creates a source that only reads the supplied file up until line <code>readToLine</code>.
	 * @param readToLineNo the line number (exclusive) up to which the reader will read the input file. If this value
	 * is 0, no limit will be set.
	 */
	public N5Source(long readToLineNo) {
		this.readToLineNo = readToLineNo;
	}
	
	@Override
	public List<String> getNextTuple() throws Exception {
		List<String> result = null;
		String line = null;
		
		if (this.readToLineNo == 0 || this.currentLine++ < this.readToLineNo) {
			line  = this.reader.readLine();
		}
		
		if (line != null) {
			String[] elements;
			String element;

			result = new ArrayList<String>();
			elements = line.split(" ");	
			// TODO: ugly hack change source interface.
			int[] indices = {0,2,3,4}; // leave away the end date at position 1
			elements[0] = Long.toString(Long.parseLong(elements[0]) * 1000); // timestamps are in unix-timestamp-format
			for (int i : indices) { 
				element = elements[i];
				element = element.replace("^^<xsd:float>", "");
				element = element.replaceAll("\"", "");
				result.add(element);
			}

		}
		
		return result;
	}

	@Override
	public InputStream buildInputStream(File file) throws FileNotFoundException {
		this.sourceId = file.getPath();
		return new FileInputStream(file.getPath());
	}

	@Override
	public void setFileInputStream(InputStream inputStream) {
		this.reader = new BufferedReader(new InputStreamReader(inputStream));
	}
	
	@Override
	public String getSourceId() {
		return this.sourceId;
	}

}
