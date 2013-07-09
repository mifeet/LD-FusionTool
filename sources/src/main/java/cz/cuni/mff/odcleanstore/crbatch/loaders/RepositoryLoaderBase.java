/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.Map;

import cz.cuni.mff.odcleanstore.crbatch.DataSource;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestrictionImpl;

/**
 * @author Jan Michelfeit
 */
public abstract class RepositoryLoaderBase {
    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    protected static final int MAX_QUERY_LIST_LENGTH = ConfigConstants.MAX_QUERY_LIST_LENGTH;

    /**
     * A random prefix for variables used in SPARQL queries so that they don't conflict
     * with variables used in named graph constraint pattern.
     */
    protected static final String VAR_PREFIX = "afdc1ea803_";
    
    /**
     * An empty restriction. 
     * To be used when no other restriction is given. 
     * Variable name is a random string to avoid conflicts.
     */
    protected static final SparqlRestriction EMPTY_RESTRICTION = new SparqlRestrictionImpl("", "308ae1cdfa_x");
    
    /** RDF data source. */
    protected final DataSource dataSource;
    
    /** Cached value returned by {@link #getPrefixDecl()}. */
    private String cachedPrefixDecl;
    
    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     */
    protected RepositoryLoaderBase(DataSource dataSource) {
        this.dataSource = dataSource;
    }
    
    /**
     * Returns a SPARQL snippet with namespace prefix declarations.
     * @return SPARQL query snippet
     */
    protected String getPrefixDecl() {
        if (cachedPrefixDecl == null) {
            cachedPrefixDecl = buildPrefixDecl(dataSource.getPrefixes());
        }
        return cachedPrefixDecl;
    }

    /**
     * Creates SPARQL snippet with prefix declarations for the given namespace prefixes.
     * @param prefixes namespace prefixes
     * @return SPARQL query snippet
     */
    private static String buildPrefixDecl(Map<String, String> prefixes) {
        if (prefixes == null) {
            return "";
        }
        StringBuilder result = new StringBuilder("");
        for (Map.Entry<String, String> entry: prefixes.entrySet()) {
            result.append("\n PREFIX ")
                .append(entry.getKey())
                .append(": <")
                .append(entry.getValue())
                .append("> ");
        }
        return result.toString();
    }
}
