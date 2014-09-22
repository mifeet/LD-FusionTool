/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;



/**
 * A resource that can be (and should be) closed after it is no longer needed.
 * @param <X> type of exception thrown by interface methods
 * @author Jan Michelfeit
 */
public interface Closeable<X extends Exception> extends AutoCloseable {
    /**
     * Releases any resources associated with this object.
     * If the object is already closed then invoking this method has no effect.
     * @throws X exception
     */
    void close() throws X;
}
