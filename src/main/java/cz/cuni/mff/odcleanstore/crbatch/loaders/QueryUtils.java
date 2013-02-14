/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.Locale;

import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;

/**
 * @TODO: javadoc, merge with DatabaseLoaderBase?
 * @author Jan Michelfeit
 */
public final class QueryUtils {
    /**
     * Only named graph having URI not starting with this prefix can be included in query result.
     * @see ODCSInternal#hiddenGraphPrefix
     */
    private static final String ENGINE_TEMP_GRAPH_PREFIX = ODCSInternal.hiddenGraphPrefix;

    /**
     * (Debug) Only named graph having URI starting with this prefix can be included in query result.
     * If the value is null, there is now restriction on named graph URIs.
     * This constant is only for debugging purposes and should be null in production environment.
     */
    private static final String GRAPH_PREFIX_FILTER = null;
    
    /**
     * SPARQL snippet restricting a variable to start with the given string.
     * Must be formatted with a string argument.
     */
    private static final String PREFIX_FILTER_CLAUSE = " FILTER (bif:starts_with(str(?%s), '%s')) ";

    /**
     * SPARQL snippet restricting a variable NOT to start with the given string.
     * Must be formatted with a string argument.
     */
    private static final String PREFIX_FILTER_CLAUSE_NEGATIVE = " FILTER (!bif:starts_with(str(?%s), '%s')) ";

    
    /**
     * Returns a SPARQL snippet restricting a named graph URI referenced by the given variable to GRAPH_PREFIX_FILTER.
     * Returns an empty string if GRAPH_PREFIX_FILTER is null.
     * @see #GRAPH_PREFIX_FILTER
     * @param graphVariable SPARQL variable name
     * @return SPARQL query snippet
     */
    public static String getGraphPrefixFilter(String graphVariable) {
        String result = String.format(Locale.ROOT, PREFIX_FILTER_CLAUSE_NEGATIVE, graphVariable, ENGINE_TEMP_GRAPH_PREFIX);
        if (GRAPH_PREFIX_FILTER != null) {
            result += String.format(Locale.ROOT, PREFIX_FILTER_CLAUSE, graphVariable, GRAPH_PREFIX_FILTER);
        }
        return result;
    }
    
    /**
     * TODO: javadoc
     * @param groupGraphPattern
     * @return
     */
    public static String preprocessGroupGraphPattern(String groupGraphPattern) {
        String result = groupGraphPattern.trim();
        if (result.startsWith("{") && result.endsWith("}")) {
            result = result.substring(1, result.length() - 1);
        }
        return result;
    }
    
    /** Disable constructor for a utility class. */
    private QueryUtils() {
    }
}
