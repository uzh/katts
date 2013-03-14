package ch.uzh.ddis.katts.evaluation;

import java.util.HashMap;

class SankeyLinkMap<E> extends HashMap<String, E>{

	private static final long serialVersionUID = 1L;
	
	public E get(String source, String destination) {
		return this.get(source+destination);
	}
	
	public void put(String source, String destination, E counter) {
		this.put(source+destination, counter);
	}
	
	public boolean containsKey(String source, String destination) {
		return this.containsKey(source+destination);
	}

}
