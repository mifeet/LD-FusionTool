/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping;

import org.openrdf.model.URI;

import java.util.Iterator;
import java.util.Set;

/**
 * Extends {@link UriMappingImpl} from the Conflict Resolution component with ability
 * to iterate over all URIs for which a mapping is explicitly defined. 
 * @author Jan Michelfeit
 */
public class UriMappingIterableImpl extends UriMappingImpl implements UriMappingIterable {
    /**
     * Creates an instance with no preferred URIs.
     */
    public UriMappingIterableImpl() {
    }

    /**
     * Creates an instance with the selected preferred URIs.
     * @param preferredURIs set of URIs preferred as canonical URIs; can be null
     */
    public UriMappingIterableImpl(Set<String> preferredURIs) {
        super(preferredURIs);
    }
    
    @Override
    public Iterator<String> iterator() {
        return getUriDFUParent().keySet().iterator();
    }

    /**
     * Add an owl:sameAs mapping for the given two URIs.
     * @param subjectUri subject of a triple with the owl:sameAs predicate
     * @param objectUri object of a triple with the owl:sameAs predicate
     */
    public void addLink(URI subjectUri, URI objectUri) {
        addLink(subjectUri.stringValue(), objectUri.stringValue());
    }
}
