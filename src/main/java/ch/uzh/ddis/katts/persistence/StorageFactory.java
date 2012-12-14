package ch.uzh.ddis.katts.persistence;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import ch.uzh.ddis.katts.persistence.backend.LocalStorage;

/**
 * This class provides methods to create storage backends depending with a certain implementation. This backends can
 * backup the state to the cluster or provide service for transferring the state to other machines in case a task is
 * moved from one machine to another one.
 * 
 * Storm does only move tasks, when the rebalancing command is executed.
 * 
 * @author Thomas Hunziker
 * 
 */
public final class StorageFactory {

	private static Map<String, Storage<?, ?>> storageInstances = new ConcurrentHashMap<String, Storage<?, ?>>();

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
	public synchronized static <K, V> Storage<K, V> createStorage(Class<? extends Storage> storageClass,
			String storageKey) throws InstantiationException, IllegalAccessException {
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
	public static <K, V> Storage<K, V> createDefaultStorage(String storageKey) throws InstantiationException,
			IllegalAccessException {
		return createStorage(LocalStorage.class, storageKey);
	}

	/**
	 * Returns all storage items for this VM.
	 * 
	 * @return
	 */
	public static Map<String, Storage<?, ?>> getAllStorages() {
		return storageInstances;
	}

}
