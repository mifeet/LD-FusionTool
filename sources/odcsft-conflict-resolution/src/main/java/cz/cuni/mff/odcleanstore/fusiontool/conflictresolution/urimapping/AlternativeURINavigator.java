/**
 *
 */
package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping;

import cz.cuni.mff.odcleanstore.fusiontool.util.ConvertingIterator;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.util.iterators.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for listing of alternative URIs based on a given mapping of URIs to canonical URIs.
 * When {@link #listAlternativeUris(String)} is called for the first time, the map of alternative
 * URIs is build in O(N log N) time and O(N) space where N is number of mapped URIs.
 * @author Jan Michelfeit
 */
public class AlternativeUriNavigator {
    private static final int EXPECTED_ALTERNATIVES = 3;

    private final UriMappingIterable uriMapping;
    private Map<String, List<String>> alternativeURIMap;

    /**
     * @param uriMapping mapping of URIs to their canonical equivalent
     */
    public AlternativeUriNavigator(UriMappingIterable uriMapping) {
        this.uriMapping = uriMapping;
    }

    /**
     * Returns iterator over all URIs that map to the same canonical URIs.
     * First call of this method may have O(N log N) complexity (N is number of mapped URIs).
     * @param uri URI
     * @return iterator over alternative URIs
     */
    @Deprecated
    public List<String> listAlternativeUris(String uri) {
        String canonicalURI = uriMapping.getCanonicalURI(uri);
        List<String> alternativeURIs = getAlternativeUriMap().get(canonicalURI);
        if (alternativeURIs == null) {
            return Collections.singletonList(uri);
        } else {
            return alternativeURIs;
        }
    }

    /**
     * Returns iterator over all URIs that map to the same canonical URIs.
     * First call of this method may have O(N log N) complexity (N is number of mapped URIs).
     * @param uri URI
     * @return iterator over alternative URIs
     */
    public List<URI> listAlternativeUris(URI uri) {
        String canonicalURI = uriMapping.getCanonicalURI(uri.stringValue());
        final List<String> alternativeURIs = getAlternativeUriMap().get(canonicalURI);
        if (alternativeURIs == null) {
            return Collections.singletonList(uri);
        } else {
            List<URI> result = new ArrayList<URI>(alternativeURIs.size());
            Iterators.addAll(new StringURIConvertingIterator(alternativeURIs), result);
            return result;
        }
    }

    /**
     * Indicates whether there exist other distinct URIs that map to the same canonical URIs as {@code uri}.
     * First call of this method may have O(N log N) complexity (N is number of mapped URIs).
     * @param uri URI
     * @return iterator over alternative URIs
     */
    @Deprecated
    public boolean hasAlternativeUris(String uri) {
        String canonicalURI = uriMapping.getCanonicalURI(uri);
        List<String> alternativeURIs = getAlternativeUriMap().get(canonicalURI);
        return alternativeURIs != null && alternativeURIs.size() > 1;
    }

    /**
     * Indicates whether there exist other distinct URIs that map to the same canonical URIs as {@code uri}.
     * First call of this method may have O(N log N) complexity (N is number of mapped URIs).
     * @param uri URI
     * @return iterator over alternative URIs
     */
    public boolean hasAlternativeUris(URI uri) {
        return hasAlternativeUris(uri.toString());
    }

    private Map<String, List<String>> getAlternativeUriMap() {
        if (alternativeURIMap == null) {
            alternativeURIMap = findAlternativeUris();
        }
        return alternativeURIMap;
    }

    private Map<String, List<String>> findAlternativeUris() {
        HashMap<String, List<String>> alternativeURIMap = new HashMap<String, List<String>>();

        for (String mappedURI : uriMapping) {
            String canonicalURI = uriMapping.getCanonicalURI(mappedURI);
            List<String> alternativeURIs = alternativeURIMap.get(canonicalURI);
            if (alternativeURIs == null) {
                alternativeURIs = new ArrayList<String>(EXPECTED_ALTERNATIVES);
                alternativeURIs.add(canonicalURI); // don't forget canonical URI, it won't show up in the iteration
                alternativeURIMap.put(canonicalURI, alternativeURIs);
            }
            alternativeURIs.add(mappedURI);
        }

        return alternativeURIMap;
    }

    private static class StringURIConvertingIterator extends ConvertingIterator<String, URI> {
        private static final ValueFactory VF = ValueFactoryImpl.getInstance();

        public StringURIConvertingIterator(List<String> alternativeURIs) {
            super(alternativeURIs.iterator());
        }

        @Override
        public URI convert(String uri) {
            return VF.createURI(uri);
        }
    }
}
