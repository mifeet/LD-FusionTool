package cz.cuni.mff.odcleanstore.crbatch.loaders;

import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * Simple closeable collection of URIs.
 * @author Jan Michelfeit
 */
public interface UriCollection extends Closeable {
    /**
     * Returns {@code true} if the collection has more elements.
     * @return {@code true} if the collection has more elements
     */
    boolean hasNext();

    /**
     * Returns an element from the collection (in no particular order) and removes it.
     * @return the removed element
     * @throws CRBatchException error
     */
    String next() throws CRBatchException;
    
    /**
     * Adds a new URI to the collection. 
     * The implementation may choose to do nothing on this call.
     * @param uri the uri to add
     */
    void add(String uri);
}
