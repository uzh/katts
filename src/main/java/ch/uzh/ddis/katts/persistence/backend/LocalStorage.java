package ch.uzh.ddis.katts.persistence.backend;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import ch.uzh.ddis.katts.persistence.eviction.strategy.Strategy;

public class LocalStorage<K, V> extends AbstractStorage<K, V> {

	private Cache<K, V> cache;

	@Override
	public void initStorage(String storageKey) {
		EmbeddedCacheManager manager = new DefaultCacheManager();
		cache = manager.getCache(storageKey);
	}

	@Override
	public V get(K key) {
		return cache.get(key);
	}

	@Override
	public void put(K key, V value) {
		cache.put(key, value);
	}

	@Override
	public void remove(K key) {
		cache.remove(key);
	}

	@Override
	public void evict(K key) {
		cache.evict(key);
	}

	@Override
	public void runEviction(Strategy strategy) {
		for (K key : cache.keySet()) {
			if (strategy.evict(key, this.get(key))) {
				this.evict(key);
			}
		}
	}
}
