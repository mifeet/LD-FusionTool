package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;


/**
 * Mapping of an URI to its equivalent canonical URI.
 * A canonical URI is single URI selected for each (weakly)
 * connected component of the owl:sameAs links graph for the URIs.
 * @author Jan Michelfeit
 */
public interface UriMapping extends cz.cuni.mff.odcleanstore.conflictresolution.URIMapping{
    /**
     * Returns a mapping to a canonical URI for the selected URI.
     * If URI has no defined mapping or is mapped to itself, returns <code>uri</code>.
     * @param uri the URI to map
     * @return the canonical URI <code>uri</code> maps to
     */
    @Deprecated
    URI mapURI(URI uri);

    /**
     * Returns the canonical URI for the given URI.
     * @param uri the URI to map
     * @return the mapped URI
     */
    String getCanonicalURI(String uri);

    /**
     * Returns the canonical version for the given Resource.
     * If URI is given, its canonical version is returned if there is one, otherwise {@code resource} is returned.
     * If blank node is given, {@code resource} is returned regardless of mapping.
     */
    Resource mapResource(Resource resource);
} 