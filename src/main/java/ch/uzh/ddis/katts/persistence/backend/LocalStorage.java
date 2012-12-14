package ch.uzh.ddis.katts.persistence.backend;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * This class provides a local storage implementation of a storage backend. It stores the data in a simple hashmap. The
 * data is not backed up in the cluster.
 * 
 * @author Thomas Hunziker
 * 
 * @param <K>
 * @param <V>
 */
public class LocalStorage<K, V> extends AbstractStorage<K, V> {

	private Map<K, V> cache;

	@Override
	public void initStorage(String storageKey) {
		cache = new HashMap<K, V>();
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
}
