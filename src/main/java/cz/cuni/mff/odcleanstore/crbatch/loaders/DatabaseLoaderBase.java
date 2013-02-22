/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.Locale;
import java.util.Map;

import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.exceptions.ConnectionException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigConstants;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.vocabulary.ODCSInternal;

/**
 * @author Jan Michelfeit
 */
public abstract class DatabaseLoaderBase {

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

    /** Database connection. */
    private VirtuosoConnectionWrapper connection;
    
    /** Database connection factory. */
    private final ConnectionFactory connectionFactory;
    
    /** Settings for SPARQL queries. */
    protected final QueryConfig queryConfig;
    
    /** Cached value returned by {@link #getGraphPrefixFilter()}. */
    private String cachedGraphPrefixFilter;
    
    /** Cached value returned by {@link #getPrefixDecl()}. */
    private String cachedPrefixDecl;
    
    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param queryConfig Settings for SPARQL queries  
     */
    protected DatabaseLoaderBase(ConnectionFactory connectionFactory, QueryConfig queryConfig) {
        this.connectionFactory = connectionFactory;
        this.queryConfig = queryConfig;
    }
    
    /**
     * Returns database connection factory.
     * @return database connection factory
     */
    protected ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }
    
    /**
     * Returns a database connection.
     * The connection is shared within this instance until it is closed.
     * @return database connection
     * @throws ConnectionException database connection error
     */
    protected VirtuosoConnectionWrapper getConnection() throws ConnectionException {
        if (connection == null) {
            connection = connectionFactory.createConnection();
        }
        return connection;
    }
    
    /**
     * Returns a SPARQL snippet restricting a named graph URI referenced by variable queryConfig.getNamedGraphRestrictionVar()
     * to  {@value GRAPH_PREFIX_FILTER}.
     * Returns an empty string if {@link GRAPH_PREFIX_FILTER} is null.
     * @see #GRAPH_PREFIX_FILTER
     * @return SPARQL query snippet
     */
    protected String getGraphPrefixFilter() {
        if (cachedGraphPrefixFilter == null) {
            cachedGraphPrefixFilter = getGraphPrefixFilter(queryConfig.getNamedGraphRestrictionVar());
        }
        return cachedGraphPrefixFilter;
    }

    /**
     * Returns a SPARQL snippet restricting a named graph URI referenced by the given variable to GRAPH_PREFIX_FILTER.
     * Returns an empty string if GRAPH_PREFIX_FILTER is null.
     * @see #GRAPH_PREFIX_FILTER
     * @param graphVariable SPARQL variable name
     * @return SPARQL query snippet
     */
    private static String getGraphPrefixFilter(String graphVariable) {
        String result = String.format(Locale.ROOT, PREFIX_FILTER_CLAUSE_NEGATIVE, graphVariable, ENGINE_TEMP_GRAPH_PREFIX);
        if (GRAPH_PREFIX_FILTER != null) {
            result += String.format(Locale.ROOT, PREFIX_FILTER_CLAUSE, graphVariable, GRAPH_PREFIX_FILTER);
        }
        return result;
    }
    
    /**
     * Returns a SPARQL snippet with namespace prefix declarations.
     * @return SPARQL query snippet
     */
    protected String getPrefixDecl() {
        if (cachedPrefixDecl == null) {
            cachedPrefixDecl = buildPrefixDecl(queryConfig.getPrefixes());
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

    /**
     * Closes an opened database connection, if any.
     */
    protected void closeConnectionQuietly() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            } catch (ConnectionException e) {
                // do nothing
            }
        }
    }
}
