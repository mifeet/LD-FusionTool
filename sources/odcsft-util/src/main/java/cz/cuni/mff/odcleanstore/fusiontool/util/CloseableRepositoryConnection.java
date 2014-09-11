package cz.cuni.mff.odcleanstore.fusiontool.util;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseableRepositoryConnection implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(CloseableRepositoryConnection.class);
    private final RepositoryConnection connection;

    public CloseableRepositoryConnection(RepositoryConnection connection) {
        this.connection = connection;
    }

    public RepositoryConnection getConnection() {
        return connection;
    }

    @Override
    public void close() throws RepositoryException {
        connection.close();
    }

    public void closeQuietly() {
        try {
            connection.close();
        } catch (RepositoryException e) {
            LOG.error("Error closing repository connection", e);
        }
    }
}
