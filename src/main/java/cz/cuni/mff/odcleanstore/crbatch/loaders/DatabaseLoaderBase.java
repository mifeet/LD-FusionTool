/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.loaders;

import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.exceptions.ConnectionException;
import cz.cuni.mff.odcleanstore.crbatch.ConfigConstants;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;

/**
 * @author Jan Michelfeit
 */
public abstract class DatabaseLoaderBase {

    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    protected static final int MAX_QUERY_LIST_LENGTH = ConfigConstants.MAX_QUERY_LIST_LENGTH;
    
    /** Database connection. */
    private VirtuosoConnectionWrapper connection;
    
    /** Database connection factory. */
    private final ConnectionFactory connectionFactory;
    
    protected DatabaseLoaderBase(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
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
