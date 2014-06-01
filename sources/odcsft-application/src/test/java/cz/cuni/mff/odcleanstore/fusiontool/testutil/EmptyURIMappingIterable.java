package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

import java.util.Collections;
import java.util.Iterator;

/**
 * An empty URI mapping. Maps every URI to itself.
 */
public class EmptyUriMappingIterable implements UriMappingIterable {
    @Override
    public URI mapURI(URI uri) {
        return uri;
    }

    @Override
    public Resource mapResource(Resource uri) {
        return uri;
    }

    @Override
    public String getCanonicalURI(String uri) {
        return uri;
    }

    @Override
    public Iterator<String> iterator() {
        return Collections.<String>emptySet().iterator();
    }
}