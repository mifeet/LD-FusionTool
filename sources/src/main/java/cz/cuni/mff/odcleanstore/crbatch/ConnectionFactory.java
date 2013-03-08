package cz.cuni.mff.odcleanstore.crbatch;

import cz.cuni.mff.odcleanstore.connection.JDBCConnectionCredentials;
import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.exceptions.ConnectionException;
import cz.cuni.mff.odcleanstore.crbatch.config.Config;

/**
 * Factory class for creating connection to a clean database instance.
 * @author Jan Michelfeit
 */
public class ConnectionFactory {
    private final String connectionString;
    private final String username;
    private final String password;
    private final Integer queryTimeout;

    /**
     * Creates a new instance.
     * @param config configuration object containing connection credentials
     */
    public ConnectionFactory(Config config) {
        this.connectionString = config.getDatabaseConnectionString();
        this.username = config.getDatabaseUsername();
        this.password = config.getDatabasePassword();
        this.queryTimeout = config.getQueryTimeout();
    }

    /**
     * Returns a new database connection.
     * @return database connection
     * @throws ConnectionException database connection error
     */
    public VirtuosoConnectionWrapper createConnection() throws ConnectionException {
        JDBCConnectionCredentials credentials = new JDBCConnectionCredentials(connectionString, username, password);
        VirtuosoConnectionWrapper connection = VirtuosoConnectionWrapper.createConnection(credentials);
        if (queryTimeout != null) {
            connection.setQueryTimeout(queryTimeout);
        }
        return connection;
    }
}
