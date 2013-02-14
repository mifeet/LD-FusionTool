package cz.cuni.mff.odcleanstore.crbatch;

import cz.cuni.mff.odcleanstore.connection.JDBCConnectionCredentials;
import cz.cuni.mff.odcleanstore.connection.VirtuosoConnectionWrapper;
import cz.cuni.mff.odcleanstore.connection.exceptions.ConnectionException;

/**
 * Factory class for creating connection to a clean database instance.
 * @author Jan Michelfeit
 */
public class ConnectionFactory {
    private final String connectionString;
    private final String username;
    private final String password;

    /**
     * Creates a new instance.
     * @param connectionString JDBC connection string
     * @param username user name
     * @param password password
     */
    public ConnectionFactory(Config config) {
        this.connectionString = config.getDatabaseConnectionString();
        this.username = config.getDatabaseUsername();
        this.password = config.getDatabasePassword();
    }

    /**
     * Returns a new database connection.
     * @return database connection
     * @throws ConnectionException database connection error
     */
    public VirtuosoConnectionWrapper createConnection() throws ConnectionException {
        JDBCConnectionCredentials credentials = new JDBCConnectionCredentials(connectionString, username, password);
        return VirtuosoConnectionWrapper.createConnection(credentials);
    }
}
