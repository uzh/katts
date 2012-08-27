package ch.uzh.ddis.katts.persistence.observer;

/**
 * This interface defines that the implementing class wants to
 * be informed, when a value is evicted from the storage.
 * 
 * @author Thomas Hunziker
 *
 * @param <S>
 * @param <K>
 * @param <V>
 */
public interface EvictObserver<K, V> extends Observer<K, V> {
	
	/**
	 * This method is called, whenever a value is evicted from the 
	 * storage.
	 * 
	 * @param key
	 */
	public void evictValue(K key);
}
