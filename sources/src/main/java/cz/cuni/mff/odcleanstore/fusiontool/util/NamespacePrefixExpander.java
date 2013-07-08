/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * Utility class for expansion of namespace prefixes.
 * @author Jan Michelfeit
 */
public class NamespacePrefixExpander {
    private final Map<String, String> nsPrefixMap;
    
    /**
     * @param nsPrefixMap Map of namespace prefixes that can be used (key is the prefix, value the expanded URI).
     */
    public NamespacePrefixExpander(Map<String, String> nsPrefixMap) {
        this.nsPrefixMap = nsPrefixMap;
    }
    
    /**
     * Expands prefixed names used as keys in the given map to absolute URIs.
     * @param map map to have keys expanded
     * @return map with keys replaced by corresponding absolute URIs
     * @throws InvalidInputException a prefix has no defined mapping
     * @param <T> type of values in the map
     */
    public <T> Map<String, T> expandKeys(Map<String, T> map) throws InvalidInputException {
        Map<String, T> mapCopy = new HashMap<String, T>(map.size());
        for (Entry<String, T> entry : map.entrySet()) {
            String key = entry.getKey();
            if (ODCSUtils.isPrefixedName(key)) {
                mapCopy.put(expandPrefix(key), entry.getValue());
            } else {
                mapCopy.put(key, entry.getValue());
            }
        }
        return mapCopy;
    }
    
    /**
     * Expands a prefixed name to a whole URI or returns its argument if its an absolute URI.
     * @param prefixedName prefixed name to expand
     * @return the expended URI or null if no mapping is found
     * @throws InvalidInputException used prefix has no mapping
     */
    public String expandPrefix(String prefixedName) throws InvalidInputException {
        if (!ODCSUtils.isPrefixedName(prefixedName)) {
            return prefixedName;
        }
        int colon = prefixedName.indexOf(':');
        if (colon < 0) {
            return prefixedName;
        }
        String prefix = prefixedName.substring(0, colon);
        String expandedPrefix = nsPrefixMap.get(prefix);
        if (expandedPrefix == null) {
            throw new InvalidInputException("Undefined namespace prefix '" + prefix + "' used in configuration.'");
        }
        return expandedPrefix + prefixedName.substring(colon + 1);
    }
}
