package cz.cuni.mff.odcleanstore.fusiontool.util;

import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CloseableRepositoryConnectionTest {
    @Test
    public void closesConnection() throws Exception {
        RepositoryConnection connection = mock(RepositoryConnection.class);
        CloseableRepositoryConnection closeableRepositoryConnection = new CloseableRepositoryConnection(connection);
        closeableRepositoryConnection.close();

        verify(connection).close();
    }

    @Test
    public void closesConnectionQuietly() throws Exception {
        RepositoryConnection connection = mock(RepositoryConnection.class);
        Mockito.doThrow(new RepositoryException()).when(connection).close();
        CloseableRepositoryConnection closeableRepositoryConnection = new CloseableRepositoryConnection(connection);
        closeableRepositoryConnection.closeQuietly();

        verify(connection).close();
    }
}