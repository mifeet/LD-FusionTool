package cz.cuni.mff.odcleanstore.crbatch.urimapping;

import java.util.Iterator;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;

/**
 * Extends {@link URIMapping} with ability to iterate over all URIs for which a mapping is explicitly defined. 
 * @author Jan Michelfeit
 */
public interface URIMappingIterable extends URIMapping, Iterable<String> {
    /**
     * Returns iterator over URIs for which a mapping is explicitly defined.
     * @return iterator over contained URIs
     */
    @Override
    Iterator<String> iterator();
}
