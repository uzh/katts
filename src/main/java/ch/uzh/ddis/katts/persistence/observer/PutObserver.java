package ch.uzh.ddis.katts.persistence.observer;

/**
 * This interface defines that the implementing class wants to be informed, when a value is stored into the storage.
 * 
 * @author Thomas Hunziker
 * 
 * @param <S>
 * @param <K>
 * @param <V>
 */
public interface PutObserver<K, V> extends Observer<K, V> {

	/**
	 * This method is called, whenever a value is updated.
	 * 
	 * @param key
	 * @param value
	 */
	public void updateValue(K key, V value);
}
