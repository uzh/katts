package ch.uzh.ddis.katts.persistence.eviction.strategy;

public interface Strategy {
	public <K, V> boolean evict(K key, V value);
}
