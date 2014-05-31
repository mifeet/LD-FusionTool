package cz.cuni.mff.odcleanstore.fusiontool.urimapping;

import cz.cuni.mff.odcleanstore.conflictresolution.URIMapping;

import java.util.Iterator;

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
