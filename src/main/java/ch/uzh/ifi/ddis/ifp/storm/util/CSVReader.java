/**
 * (C) 2013 University of Zurich, Department of Informatics
 *     http://www.ifi.uzh.ch
 *
 * This program was produced in the context of the ViSTA-TV project.
 *     http://vista-tv.eu  
 *
 * The ViSTA-TV project has received funding from the 
 *     European Union Seventh Framework Programme FP7/2007-2011 
 *     under grant agreement n° 296126.”
 *
 * This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Affero General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 * 
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Affero General Public License for more details.
 * 
 *     You should have received a copy of the GNU Affero General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */
package ch.uzh.ifi.ddis.ifp.storm.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * 
 * 
 * @author Thomas Scharrenbach
 * @version 0.0.1
 * @since 0.0.1
 * 
 */
public class CSVReader implements Runnable {

	public static final Logger _LOG = Logger.getLogger(CSVReader.class
			.getCanonicalName());

	public static char DEFAULT_SEPARATOR_CHAR = ',';

	public static long DEFAULT_BUFFER_TIMEOUT = 10;

	//
	//
	//

	private boolean _interrupted;

	private boolean _running;

	private BufferedReader _reader;

	private BlockingQueue<List<Object>> _buffer;

	private char _separator;

	private long _bufferTimeout;

	//
	//
	//

	public CSVReader(BufferedReader reader,
			BlockingQueue<List<Object>> buffer) {
		setReader(reader);
		setBuffer(buffer);
		setSeparator(DEFAULT_SEPARATOR_CHAR);
		setBufferTimeout(DEFAULT_BUFFER_TIMEOUT);
	}

	//
	//
	//

	/**
	 * Reads a single line from the CSV file.
	 * 
	 * @return
	 * @throws IOException
	 */
	public synchronized List<Object> readLine() throws IOException {
		final String line = getReader().readLine();
		return line == null ? null : Arrays.asList((Object[]) line.split(String
				.valueOf(getSeparator())));
	}

	/**
	 * 
	 */
	@Override
	public void run() {
		if (isRunning()) {
			throw new RuntimeException();
		}
		setRunning(true);
		setInterrupted(false);
		List<Object> values = null;
		final BlockingQueue<List<Object>> buffer = getBuffer();
		final long timeout = getBufferTimeout();
		long counter = 0;

		try {
			// While not interrupted, a line could be read and the element could
			// be inserted,
			// 1) read the file line by line,
			// 2) split the line by the separator char
			// 3) add the list of strings to the buffer.
			while (!isInterrupted() && (values = readLine()) != null) {
				buffer.offer(values, (timeout * (++counter)),
						TimeUnit.MILLISECONDS);
			}
		} catch (IOException e) {
			_LOG.log(Level.SEVERE, String.format(Messages.ERROR_IO), e);
			setInterrupted(true);
		} catch (InterruptedException e) {
			_LOG.log(Level.WARNING, String.format(Messages.WARN_INTERRUPTED,
					"offering elements to buffer"));
			setInterrupted(true);
		}
		if (isInterrupted()) {
			_LOG.log(Level.WARNING, String.format(Messages.WARN_INTERRUPTED,
					"reading from the CSV file"));
		}
		setRunning(false);
	}

	/**
	 * @return the interrupted
	 */
	public boolean isInterrupted() {
		return _interrupted;
	}

	/**
	 * @param interrupted
	 *            the interrupted to set
	 */
	public void setInterrupted(boolean interrupted) {
		_interrupted = interrupted;
	}

	/**
	 * @return the running
	 */
	public boolean isRunning() {
		return _running;
	}

	/**
	 * @param running
	 *            the running to set
	 */
	public void setRunning(boolean running) {
		_running = running;
	}

	/**
	 * @return the reader
	 */
	public BufferedReader getReader() {
		return _reader;
	}

	/**
	 * @param reader
	 *            the reader to set
	 */
	public void setReader(BufferedReader reader) {
		_reader = reader;
	}

	/**
	 * @return the buffer
	 */
	public BlockingQueue<List<Object>> getBuffer() {
		return _buffer;
	}

	/**
	 * @param buffer
	 *            the buffer to set
	 */
	public void setBuffer(BlockingQueue<List<Object>> buffer) {
		_buffer = buffer;
	}

	/**
	 * @return the separator
	 */
	public char getSeparator() {
		return _separator;
	}

	/**
	 * @param separator
	 *            the separator to set
	 */
	public void setSeparator(char separator) {
		_separator = separator;
	}

	/**
	 * @return the bufferTimeout
	 */
	public long getBufferTimeout() {
		return _bufferTimeout;
	}

	/**
	 * @param bufferTimeout
	 *            the bufferTimeout to set
	 */
	public void setBufferTimeout(long bufferTimeout) {
		_bufferTimeout = bufferTimeout;
	}

	/**
	 * 
	 */
	public synchronized void interrupt() {
		setInterrupted(true);
	}

}
