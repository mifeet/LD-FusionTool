package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;

import java.util.Iterator;

/**
 * Extends {@link UriMapping} with ability to iterate over all URIs for which a mapping is explicitly defined.
 * @author Jan Michelfeit
 */
public interface UriMappingIterable extends UriMapping, Iterable<String> {
    /**
     * Returns iterator over URIs for which a mapping is explicitly defined.
     * @return iterator over contained URIs
     */
    @Override
    Iterator<String> iterator();
}
