package ch.uzh.ddis.katts.evaluation;

class Counter {
	long counter = 0;
	
	private String source;
	private String destination;
	
	public Counter(String source, String destination) {
		this.source = source;
		this.destination = destination;
	}

	public void increase(long number) {
		counter = counter + number;
	}

	public long getCounter() {
		return counter;
	}

	public String getSource() {
		return source;
	}

	public String getDestination() {
		return destination;
	}
}