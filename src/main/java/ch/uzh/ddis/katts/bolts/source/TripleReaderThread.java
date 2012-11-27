package ch.uzh.ddis.katts.bolts.source;

import java.io.Serializable;

import backtype.storm.utils.Utils;

class TripleReaderThread implements Runnable, Serializable {

	private static final long serialVersionUID = 1L;
	
	private FileTripleReader bolt;
	
	
	public TripleReaderThread(FileTripleReader bolt) {
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