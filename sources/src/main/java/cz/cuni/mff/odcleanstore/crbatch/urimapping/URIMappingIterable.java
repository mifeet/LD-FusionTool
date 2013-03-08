package cz.cuni.mff.odcleanstore.crbatch.urimapping;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMapping;

/**
 * Extends {@link URIMapping} with ability to iterate over all URIs for which a mapping is explicitly defined. 
 * @author Jan Michelfeit
 */
public interface URIMappingIterable extends URIMapping, Iterable<String> {

}
