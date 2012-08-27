package ch.uzh.ddis.katts.persistence;

import java.util.HashMap;
import java.util.Map;

import ch.uzh.ddis.katts.persistence.backend.LocalStorage;


public class StorageFactory {
	
	private static Map<String, Storage<?,?>> storageInstances = new HashMap<String, Storage<?,?>>();

	/**
	 * This method creates a storage instance for the given key using the storage class provided. If such a storage 
	 * instance already exists, a reference to this instance will be returned.
	 * 
	 * This method provides thread safety through synchronization.
	 * 
	 * @param storageClass
	 * @param storageKey
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public synchronized static <K, V> Storage<K, V> createStorage(Class<? extends Storage> storageClass, String storageKey) throws InstantiationException, IllegalAccessException {	
	    Storage storageInstance = storageInstances.get(storageKey);
		if (storageInstance == null) {
			storageInstance = storageClass.newInstance();
			storageInstance.initStorage(storageKey);
			storageInstances.put(storageKey, storageInstance);
		}
		
		return storageInstance;
	}
	
	/**
	 * This method creates a storage instance for the given key using the default storage class. If such a storage 
     * instance already exists, a reference to this instance will be returned.
     * 
	 * This method provides thread safety through synchronization.
	 * 
	 * @param storageKey
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public static <K, V> Storage<K, V> createDefaultStorage(String storageKey) throws InstantiationException, IllegalAccessException {
		return createStorage(LocalStorage.class, storageKey);
	}
	
	public static Map<String, Storage<?,?>> getAllStorages() {
		return storageInstances;
	}
	
}
