/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.loaders;

import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.exceptions.ConnectionException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.config.ConfigConstants;

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
    
    /** Database connection. */
    private VirtuosoConnectionWrapper connection;
    
    /** Database connection factory. */
    private final ConnectionFactory connectionFactory;
    
    /** SPARQL group graph pattern limiting source payload named graphs. */
    protected final String ngRestrictionPattern;
    
    /** SPARQL query variable name referring to the filtered named graph in {@link #namedGraphRestrictionPattern}. */
    protected final String ngRestrictionVar;
    
    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param ngRestrictionPattern SPARQL group graph pattern limiting source payload named graphs
     * @param ngRestrictionVar named of SPARQL variable representing the payload graph in namedGraphConstraintPattern  
     */
    protected DatabaseLoaderBase(ConnectionFactory connectionFactory, String ngRestrictionPattern, String ngRestrictionVar) {
        this.connectionFactory = connectionFactory;
        this.ngRestrictionPattern = ngRestrictionPattern;
        this.ngRestrictionVar = ngRestrictionVar;
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
