/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.urimapping;

import java.util.Iterator;
import java.util.Set;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;

/**
 * Extends {@link URIMappingImpl} from the Conflict Resolution component with ability
 * to iterate over all URIs for which a mapping is explicitly defined. 
 * @author Jan Michelfeit
 */
public class URIMappingIterableImpl extends URIMappingImpl implements URIMappingIterable {
    /**
     * Creates an instance with no preferred URIs.
     */
    public URIMappingIterableImpl() {
    }

    /**
     * Creates an instance with the selected preferred URIs.
     * @param preferredURIs set of URIs preferred as canonical URIs; can be null
     */
    public URIMappingIterableImpl(Set<String> preferredURIs) {
        super(preferredURIs);
    }
    
    /**
     * Returns iterator over URIs for which a mapping is explicitly defined.
     * @return iterator over contained URIs
     */
    @Override
    public Iterator<String> iterator() {
        return getUriDFUParent().keySet().iterator();
    }
}
