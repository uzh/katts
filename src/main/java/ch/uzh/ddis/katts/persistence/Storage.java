package ch.uzh.ddis.katts.persistence;

public interface Storage<K, V>{
	
	public void initStorage(String storageKey);
	
	public V get(K key);
	
	public void put(K key, V value);
	
	public void remove(K key);
	
	public void evict(K key);
	
}
