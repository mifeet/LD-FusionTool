package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

import java.util.Collections;
import java.util.Iterator;

/**
 * An empty URI mapping. Maps every URI to itself.
 */
public class EmptyUriMappingIterable implements UriMappingIterable {
    private static final EmptyUriMappingIterable INSTANCE = new EmptyUriMappingIterable();

    /**
     * Return the shared default instance of this class.
     * @return shared instance of this class
     */
    public static UriMapping getInstance() {
        return INSTANCE;
    }

    @Override
    public URI mapURI(URI uri) {
        return uri;
    }

    @Override
    public String getCanonicalURI(String uri) {
        return uri;
    }

    @Override
    public Resource mapResource(Resource resource) {
        return resource;
    }

    @Override
    public Iterator<String> iterator() {
        return Collections.emptyIterator();
    }
}