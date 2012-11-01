package ch.uzh.ddis.katts.persistence.observer;

/**
 * This interface is only an abstract interface for GetObserver and PutObserver. 
 * You should either implement GetObserver, PutObserver or both. But you should 
 * not directly implement the Observer.
 * 
 * @author Thomas Hunziker
 *
 * @param <S>
 * @param <K>
 * @param <V>
 */
public interface Observer<K, V> {

}
