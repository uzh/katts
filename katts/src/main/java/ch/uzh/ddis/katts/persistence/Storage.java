package ch.uzh.ddis.katts.persistence;

import ch.uzh.ddis.katts.persistence.eviction.strategy.Strategy;

public interface Storage<K, V>{
	
	public void initStorage(String storageKey);
	
	public V get(K key);
	
	public void put(K key, V value);
	
	public void remove(K key);
	
	public void evict(K key);
	
	public void runEviction(Strategy strategy);
	
}
