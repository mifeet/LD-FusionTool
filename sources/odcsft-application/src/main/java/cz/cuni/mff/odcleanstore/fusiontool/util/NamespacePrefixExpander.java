/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.InvalidInputException;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import java.util.Map;

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
     * Expands a prefixed name to a whole URI or returns its argument if its an absolute URI.
     * @param prefixedName prefixed name to expand
     * @return the expended URI or null if no mapping is found
     * @throws InvalidInputException used prefix has no mapping
     */
    private String expandPrefix(String prefixedName) throws InvalidInputException {
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

    /**
     * Converts the given string to {@link org.openrdf.model.URI} with expansion of prefixed names.
     * @param prefixedNameOrUri prefixed name to expand or valid URI strign
     * @return the expended URI or null if no mapping is found
     * @throws InvalidInputException used prefix has no mapping
     */
    public URI convertToUriWithExpansion(String prefixedNameOrUri) throws InvalidInputException {
        try {
            return new URIImpl(expandPrefix(prefixedNameOrUri));
        } catch (InvalidInputException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException(e.getMessage(), e);
        }
    }
}
