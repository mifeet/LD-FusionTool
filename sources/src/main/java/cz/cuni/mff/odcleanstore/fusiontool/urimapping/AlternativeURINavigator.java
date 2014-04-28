/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.urimapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class for listing of alternative URIs based on a given mapping of URIs to canonical URIs.
 * When {@link #listAlternativeURIs(String)} is called for the first time, the map of alternative 
 * URIs is build in O(N log N) time and O(N) space where N is number of mapped URIs.
 * 
 * @author Jan Michelfeit
 */
public class AlternativeURINavigator {
    private static final int EXPECTED_ALTERNATIVES = 3;
    
    private final URIMappingIterable uriMapping;
    private Map<String, List<String>> alternativeURIMap;
    
    /**
     * @param uriMapping mapping of URIs to their canonical equivalent
     */
    public AlternativeURINavigator(URIMappingIterable uriMapping) {
        this.uriMapping = uriMapping;
    }
    
    /**
     * Returns iterator over all URIs that map to the same canonical URIs.
     * First call of this method has O(N log N) complexity (N is number of mapped URIs).
     * @param uri URI
     * @return iterator over alternative URIs
     */
    public List<String> listAlternativeURIs(String uri) {
        String canonicalURI = uriMapping.getCanonicalURI(uri);
        List<String> alternativeURIs = getAlternativeURIMap().get(canonicalURI);
        if (alternativeURIs == null) {
            return Collections.singletonList(uri);
        } else {
            return alternativeURIs;
        }
    }
    
    private Map<String, List<String>> getAlternativeURIMap() {
        if (alternativeURIMap == null) {
            alternativeURIMap = findAlternativeURIs();
        }
        return alternativeURIMap;
    }

    private Map<String, List<String>> findAlternativeURIs() {
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
}
