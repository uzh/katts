package ch.uzh.ddis.katts.bolts.source;

import java.io.Serializable;

import backtype.storm.utils.Utils;

/**
 * This thread reads in the data by executing constantly the nextTuple() method of the {@link FileTripleReader}. When no
 * result was found in the file, the reader sleeps for 10 seconds, before another try is started.
 * 
 * @author Thomas Hunziker
 * 
 */
@SuppressWarnings("serial")
class NTupleReaderThread implements Runnable, Serializable {


	private FileNTupleReader bolt;

	/**
	 * Constructor of the Runnable.
	 * 
	 * @param bolt
	 *            The {@link FileTripleReader} to read from.
	 */
	public NTupleReaderThread(FileNTupleReader bolt) {
		this.bolt = bolt;
	}

	@Override
	public void run() {
		while (true) {
			if (!bolt.nextTuple()) {
				// when we got no new result, we sleep to prevent blocking the processor.
				Utils.sleep(10000);
			}
		}
	}
}