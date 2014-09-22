package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;

import java.io.IOException;

/**
 * Simple closeable iteration over URIs.
 * @author Jan Michelfeit
 */
public interface UriCollection extends Closeable<IOException> {
    /**
     * Returns {@code true} if the collection has more elements.
     * @return {@code true} if the collection has more elements
     */
    boolean hasNext();

    /**
     * Returns an element from the collection (in no particular order) and removes it.
     * @return the removed element
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     */
    String next() throws LDFusionToolException;
    
    /**
     * Adds a new URI to the collection. 
     * The implementation may choose to do nothing on this call.
     * @param uri the uri to add
     */
    void add(String uri);
}
