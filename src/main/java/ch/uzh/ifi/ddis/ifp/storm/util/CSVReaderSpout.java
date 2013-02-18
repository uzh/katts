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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

/**
 * The CSV is an input spout that reads from a CSV file.
 * <p>
 * The output fields are determined by the first line of the CSV file. The field
 * separator and field-enclosing characters are configurable. By default, the
 * fields are assumed to be separated by commas and not enclosed by anything.
 * </p>
 * <p>
 * Note that the reader trims the input fields.
 * </p>
 * 
 * @author Thomas Scharrenbach
 * @version 0.0.1
 * @since 0.0.1
 * 
 */
public class CSVReaderSpout implements IRichSpout {

	public static final Logger _LOG = Logger.getLogger(CSVReaderSpout.class
			.getCanonicalName());

	/**
	 * The default polling period in milliseconds.
	 */
	public static final long DEFAULT_POLLING_PERIOD = 100;

	/**
	 * The default size of the read buffer.
	 */
	public static final int DEFAULT_BUFFER_SIZE = 1000;

	/**
	 * 
	 */
	private static final long serialVersionUID = 1279765083247127563L;

	//
	//
	//

	private transient BufferedReader _reader;

	private transient Thread _readingThread;

	private transient CSVReader _csvReader;

	private URL _url;

	private transient BlockingQueue<List<Object>> _buffer;

	private long _pollingPeriod;

	private long _lastPeriod;

	private int _bufferSize;

	private SpoutOutputCollector _outputCollector;

	//
	//
	//

	public CSVReaderSpout(URL url) {
		setUrl(url);
		setBufferSize(DEFAULT_BUFFER_SIZE);
		setPollingPeriod(DEFAULT_POLLING_PERIOD);
	}

	//
	//
	//

	/**
	 * <p>
	 * <ul>
	 * <li>Open the stream behind the URL and initialize the reader.</li>
	 * </ul>
	 * </p>
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 *      backtype.storm.task.TopologyContext,
	 *      backtype.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		setOutputCollector(collector);
		setBuffer(new ArrayBlockingQueue<List<Object>>(getBufferSize()));
		InputStream inputStream = null;
		BufferedReader reader = null;
		final URL url = getUrl();
		try {
			_LOG.log(Level.INFO,
					String.format("Opening URL %s for reading", url.toString()));
			inputStream = url.openStream();
			reader = new BufferedReader(new InputStreamReader(inputStream));
			setReader(reader);
			setCsvReader(new CSVReader(reader, getBuffer()));
			getCsvReader().readLine();
			setReadingThread(new Thread(getCsvReader()));
			getReadingThread().start();
			setLastPeriod(System.currentTimeMillis());

		} catch (IOException e) {
			final String errorMessage = String.format(Messages.ERROR_OPEN_URL,
					url.toString());
			_LOG.log(Level.SEVERE, errorMessage, e);
			throw new RuntimeException(errorMessage, e);
		}
	}

	/**
	 * Interrupt the thread by calling {@link CSVReaderSpout#deactivate()} and
	 * try to close the buffered reader.
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		// Interrupt the csv reader.
		try {
			deactivate();
		}
		// Close the actual buffered reader under any circumstances.
		finally {
			final Reader reader = getReader();
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					_LOG.log(Level.SEVERE,
							String.format(Messages.ERROR_CLOSING_READER), e);
				}
			}
		}
	}

	/**
	 * Starts the reading thread.
	 * 
	 * @see backtype.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
	}

	/**
	 * Interrupts the {@link CSVReader} and waits for the reading {@link Thread}
	 * to join. This method is non-blocking.
	 * 
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		getCsvReader().interrupt();
		try {
			getReadingThread().join(10);
		} catch (InterruptedException e) {
			_LOG.log(Level.WARNING, String.format(Messages.WARN_INTERRUPTED,
					"waiting for csv reader thread to join"));
		}
	}

	/**
	 * Emits a tuple from the buffer every polling period milliseconds.
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		long timeout = Math.min(10, getPollingPeriod());
		// Sleep to match the polling time.
		try {
			Thread.sleep(Math.max(1, System.currentTimeMillis()
					- (getLastPeriod() + getPollingPeriod())));
			setLastPeriod(System.currentTimeMillis());
		} catch (InterruptedException e1) {
			;
		}
		try {
			List<Object> values = getBuffer().poll(timeout,
					TimeUnit.MILLISECONDS);
			if (values != null) {
				getOutputCollector().emit(values);
			}

		} catch (InterruptedException e) {
			_LOG.log(Level.WARNING,
					String.format(Messages.WARN_INTERRUPTED, "Polling buffer."));
		}
	}

	/**
	 * Does nothing.
	 * 
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
		return;
	}

	/**
	 * Does nothing.
	 * 
	 * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object msgId) {
		return;
	}

	/**
	 * Read the first line of the CSV document and treat the values as field
	 * names.
	 * 
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 *      .topology.OutputFieldsDeclarer)
	 * 
	 * @throws RuntimeException
	 *             In case of an IOException whilst reading from the CSV file.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		setBuffer(new ArrayBlockingQueue<List<Object>>(getBufferSize()));
		InputStream inputStream = null;
		BufferedReader reader = null;
		final URL url = getUrl();
		try {
			inputStream = url.openStream();
			reader = new BufferedReader(new InputStreamReader(inputStream));
			setReader(reader);
			setCsvReader(new CSVReader(reader, getBuffer()));
			final List<String> fields = new ArrayList<String>();
			for (Object fieldName : getCsvReader().readLine()) {
				fields.add(fieldName.toString());
			}
			declarer.declare(new Fields(fields));
		} catch (IOException e) {
			final String errorMessage = String.format(Messages.ERROR_OPEN_URL,
					url.toString());
			_LOG.log(Level.SEVERE, errorMessage, e);
			throw new RuntimeException(errorMessage, e);
		}
	}

	/**
	 * @return null;
	 * 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	//
	// Getters and setters.
	//

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
	 * @return the url
	 */
	public URL getUrl() {
		return _url;
	}

	/**
	 * @param url
	 *            the url to set
	 */
	public void setUrl(URL url) {
		_url = url;
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
	 * @return the readingThread
	 */
	public Thread getReadingThread() {
		return _readingThread;
	}

	/**
	 * @param readingThread
	 *            the readingThread to set
	 */
	public void setReadingThread(Thread readingThread) {
		_readingThread = readingThread;
	}

	/**
	 * @return the csvReader
	 */
	public CSVReader getCsvReader() {
		return _csvReader;
	}

	/**
	 * @param csvReader
	 *            the csvReader to set
	 */
	public void setCsvReader(CSVReader csvReader) {
		_csvReader = csvReader;
	}

	/**
	 * @return the pollingPeriod (ms)
	 */
	public long getPollingPeriod() {
		return _pollingPeriod;
	}

	/**
	 * @param pollingPeriod
	 *            the pollingPeriod to set (ms)
	 */
	public void setPollingPeriod(long pollingPeriod) {
		_pollingPeriod = pollingPeriod;
	}

	/**
	 * @return the lastPeriod
	 */
	public long getLastPeriod() {
		return _lastPeriod;
	}

	/**
	 * @param lastPeriod
	 *            the lastPeriod to set
	 */
	public void setLastPeriod(long lastPeriod) {
		_lastPeriod = lastPeriod;
	}

	/**
	 * @return the bufferSize
	 */
	public int getBufferSize() {
		return _bufferSize;
	}

	/**
	 * @param bufferSize
	 *            the bufferSize to set
	 */
	public void setBufferSize(int bufferSize) {
		_bufferSize = bufferSize;
	}

	/**
	 * @return the outputCollector
	 */
	public SpoutOutputCollector getOutputCollector() {
		return _outputCollector;
	}

	/**
	 * @param outputCollector
	 *            the outputCollector to set
	 */
	public void setOutputCollector(SpoutOutputCollector outputCollector) {
		_outputCollector = outputCollector;
	}

}
