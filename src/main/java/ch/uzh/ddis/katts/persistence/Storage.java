package ch.uzh.ddis.katts.persistence;

/**
 * This interface defines a Storage item. An item consists always of a key and a value. This interface provides
 * functionalities to access the data with generics.
 * 
 * @author Thomas Hunziker
 * 
 * @param <K> The key type
 * @param <V> The value type
 */
public interface Storage<K, V> {

	/**
	 * Inits the storage item. This is used to inform the backend about the item.
	 * 
	 * @param storageKey
	 */
	public void initStorage(String storageKey);

	/**
	 * Returns the data for the given key.
	 * 
	 * @param key
	 * @return
	 */
	public V get(K key);

	/**
	 * This method stores a new item in the storage.
	 * 
	 * @param key
	 * @param value
	 */
	public void put(K key, V value);

	/**
	 * Removes a certain key from the storage.
	 * 
	 * @param key
	 */
	public void remove(K key);

}
