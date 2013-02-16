/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;


/**
 * A resource that can be (and should be) closed after it is no longer needed.
 * @author Jan Michelfeit
 */
public interface Closeable {
    /**
     * Releases any resources associated with this object.
     * If the object is already closed then invoking this method has no effect.
     */
    void close();
}
