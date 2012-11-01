package ch.uzh.ddis.katts.persistence.observer;

/**
 * This interface defines that the implementing class wants to
 * be informed, when a value is retrieved from the storage.
 * 
 * @author Thomas Hunziker
 *
 * @param <S>
 * @param <K>
 * @param <V>
 */
public interface GetObserver<K, V> extends Observer<K, V>{
	
	/**
	 * This method is invoked when a value is loaded from the storage.
	 * 
	 * @param key
	 * @param value
	 */
	public void retrieveValue(K key, V value);
}
