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

	@Override
	public List<String> getNextTuple() throws Exception {
		List<String> result = null;
		String line = this.reader.readLine();
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
		return new FileInputStream(file.getPath());
	}

	@Override
	public void setFileInputStream(InputStream inputStream) {
		this.reader = new BufferedReader(new InputStreamReader(inputStream));
	}

}
