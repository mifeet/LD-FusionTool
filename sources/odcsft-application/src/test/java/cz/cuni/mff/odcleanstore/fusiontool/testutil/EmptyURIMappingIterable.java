package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import org.openrdf.model.URI;

import java.util.Collections;
import java.util.Iterator;

/**
 * An empty URI mapping. Maps every URI to itself.
 */
public class EmptyURIMappingIterable implements URIMappingIterable {
    @Override
    public URI mapURI(URI uri) {
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