/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationType;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
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
     * Expands prefixed names in the given aggregation settings to full URIs.
     * @param aggregationSpec aggregation settings where property names are expanded
     * @return new aggregation settings
     * @throws InvalidInputException a prefix has no defined mapping
     */
    public AggregationSpec expandPropertyNames(AggregationSpec aggregationSpec) throws InvalidInputException {

        if (aggregationSpec.getPropertyAggregations().isEmpty() && aggregationSpec.getPropertyMultivalue().isEmpty()) {
            return aggregationSpec;
        }
        AggregationSpec result = aggregationSpec.shallowClone();

        Map<String, EnumAggregationType> newPropertyAggregations = new TreeMap<String, EnumAggregationType>();
        for (Entry<String, EnumAggregationType> entry : aggregationSpec.getPropertyAggregations().entrySet()) {
            String property = entry.getKey();
            if (ODCSUtils.isPrefixedName(property)) {
                newPropertyAggregations.put(expandPrefix(property), entry.getValue());
            } else {
                newPropertyAggregations.put(property, entry.getValue());
            }
        }
        result.setPropertyAggregations(newPropertyAggregations);

        Map<String, Boolean> newPropertyMultivalue = new TreeMap<String, Boolean>();
        for (Entry<String, Boolean> entry : aggregationSpec.getPropertyMultivalue().entrySet()) {
            String property = entry.getKey();
            if (ODCSUtils.isPrefixedName(property)) {
                newPropertyMultivalue.put(expandPrefix(property), entry.getValue());
            } else {
                newPropertyMultivalue.put(property, entry.getValue());
            }
        }
        result.setPropertyMultivalue(newPropertyMultivalue);

        return result;
    }
    
    /**
     * Expands a prefixed name to a whole URI.
     * Assumes that if the given string contains a ':', it <em>is</em> a prefixed name.
     * @param prefixedName prefixed name to expand
     * @return the expended URI or null if no mapping is found
     * @throws InvalidInputException used prefix has no mapping
     */
    public String expandPrefix(String prefixedName) throws InvalidInputException {
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
