package cz.cuni.mff.odcleanstore.fusiontool.util;

import com.google.code.externalsorting.StringSizeEstimator;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import static org.junit.Assert.*;

public class StatementSizeEstimatorTest {
    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

    @Test
    public void estimatedSizeOfWithLiteral() throws Exception {
        // Arrange
        Statement statement = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createBNode("http://a"),
                VALUE_FACTORY.createURI("http://b"),
                VALUE_FACTORY.createLiteral("abc", VALUE_FACTORY.createURI("http://c")),
                VALUE_FACTORY.createURI("http://d"));

        // Act
        long size = StatementSizeEstimator.estimatedSizeOf(statement);

        // Assert
        long stringsSize = 4 * StringSizeEstimator.estimatedSizeOf("http://a") + StringSizeEstimator.estimatedSizeOf("abc");
        long minOverhead = 6 * 8 + 12 * 4 + 4 * 8; // objects + object references + int fields
        long maxOverhead = 6 * 16 + 12 * 8 + 4 * 8;
        assertTrue(size <= maxOverhead + stringsSize);
        assertTrue(size >= minOverhead + stringsSize);
    }

    @Test
    public void estimatedSizeOfWithoutLiteral() throws Exception {
        // Arrange
        Statement statement = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createBNode("http://a"),
                VALUE_FACTORY.createURI("http://b"),
                VALUE_FACTORY.createURI("http://c"),
                VALUE_FACTORY.createURI("http://d"));

        // Act
        long size = StatementSizeEstimator.estimatedSizeOf(statement);

        // Assert
        long stringsSize = 4 * StringSizeEstimator.estimatedSizeOf("http://a");
        long minOverhead = 5 * 8 + 9 * 4 + 4 * 8; // objects + object references + int fields
        long maxOverhead = 5 * 16 + 9 * 8 + 4 * 8;
        assertTrue(size <= maxOverhead + stringsSize);
        assertTrue(size >= minOverhead + stringsSize);
    }
}