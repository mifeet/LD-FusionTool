package cz.cuni.mff.odcleanstore.fusiontool.util;

import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class ODCSFusionToolAppUtilsTest {
    @Test
    public void closeQuietlyClosesResourceAndSwallowsException() throws Exception {
        Closeable<Exception> closeable = mock(Closeable.class);
        Mockito.doThrow(new Exception("test exception")).when(closeable).close();

        ODCSFusionToolAppUtils.closeQuietly(closeable);

        Mockito.verify(closeable).close();
    }

    @Test
    public void closeQuietlyDoesNotThrowForNullResource() throws Exception {
        ODCSFusionToolAppUtils.closeQuietly(null);
    }
}