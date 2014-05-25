package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.collect.ImmutableSet;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.SpogComparator;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort.ExternalSortingInputLoaderPreprocessor;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.EmptyURIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.StatementCollector;

import java.util.ArrayList;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ExternalSortingInputLoaderPreprocessorTest {
    public static final SpogComparator COMPARATOR = new SpogComparator();
    public static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();

    @Test
    public void producesQuadsWithAppliedUriMapping() throws Exception {
        // Arrange
        ArrayList<Statement> statements = new ArrayList<Statement>();
        statements.add(createHttpStatement("s3", "p1", "o3", "g3"));
        statements.add(createHttpStatement("s1", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s2", "p2", "o2", "g2"));

        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(ImmutableSet.of(
                createHttpUri("sx").toString(),
                createHttpUri("px").toString(),
                createHttpUri("ox").toString(),
                createHttpUri("gx").toString()));
        uriMapping.addLink(createHttpUri("s1").toString(), createHttpUri("sx").toString());
        uriMapping.addLink(createHttpUri("s2").toString(), createHttpUri("sx").toString());
        uriMapping.addLink(createHttpUri("p1").toString(), createHttpUri("px").toString());
        uriMapping.addLink(createHttpUri("p2").toString(), createHttpUri("px").toString());
        uriMapping.addLink(createHttpUri("o1").toString(), createHttpUri("ox").toString());
        uriMapping.addLink(createHttpUri("o2").toString(), createHttpUri("ox").toString());
        uriMapping.addLink(createHttpUri("g1").toString(), createHttpUri("gx").toString());
        uriMapping.addLink(createHttpUri("g2").toString(), createHttpUri("gx").toString());

        // Act
        ArrayList<Statement> result = collectResultsFromPreprocessor(statements, uriMapping, Long.MAX_VALUE);

        // Assert
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0), contextAwareStatementIsEqual(createHttpStatement("s3", "px", "o3", "g3")));
        assertThat(result.get(1), contextAwareStatementIsEqual(createHttpStatement("sx", "px", "ox", "g1")));
        assertThat(result.get(2), contextAwareStatementIsEqual(createHttpStatement("sx", "px", "ox", "g2")));
    }

    @Test
    public void sortsQuadsWhenLargeMemoryLimit() throws Exception {
        // Arrange
        ArrayList<Statement> statements = new ArrayList<Statement>();
        statements.add(createHttpStatement("s2", "p2", "o2", "g3"));
        statements.add(createHttpStatement("s1", "p1", "o1", "g2"));
        statements.add(createHttpStatement("s1", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s2", "p3", "o2", "g3"));

        URIMappingIterable uriMapping = new EmptyURIMappingIterable();

        // Act
        ArrayList<Statement> result = collectResultsFromPreprocessor(statements, uriMapping, Long.MAX_VALUE);

        // Assert
        assertThat(result.size(), equalTo(4));
        assertThat(result.get(0), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g1")));
        assertThat(result.get(1), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g2")));
        assertThat(result.get(2), contextAwareStatementIsEqual(createHttpStatement("s2", "p2", "o2", "g3")));
        assertThat(result.get(3), contextAwareStatementIsEqual(createHttpStatement("s2", "p3", "o2", "g3")));
    }

    @Test
    public void flushesIntermediateResultsWhenLowMemoryLimit() throws Exception {
        // Arrange
        URIMappingIterable uriMapping = new EmptyURIMappingIterable();
        RDFHandler handlerMock = Mockito.mock(RDFHandler.class);

        // Act & assert
        ExternalSortingInputLoaderPreprocessor preprocessor = new ExternalSortingInputLoaderPreprocessor(uriMapping, handlerMock, 1, VALUE_FACTORY, COMPARATOR, false);
        preprocessor.startRDF();

        Statement statement1 = createStatement();
        preprocessor.handleStatement(statement1);

        Statement statement2 = createStatement();
        preprocessor.handleStatement(statement2);
        Mockito.verify(handlerMock).handleStatement(statement1);

        Statement statement3 = createStatement();
        preprocessor.handleStatement(statement3);
        Mockito.verify(handlerMock).handleStatement(statement2);

        preprocessor.endRDF();
        Mockito.verify(handlerMock).handleStatement(statement3);
    }

    @Test
    public void respectsSetContext() throws Exception {
        // Arrange
        URIMappingIterable uriMapping = new EmptyURIMappingIterable();

        // Act
        ArrayList<Statement> result = new ArrayList<Statement>();
        ExternalSortingInputLoaderPreprocessor preprocessor = new ExternalSortingInputLoaderPreprocessor(uriMapping, new StatementCollector(result), Long.MAX_VALUE, ValueFactoryImpl.getInstance(), COMPARATOR, false);
        preprocessor.startRDF();

        Statement statement1 = VALUE_FACTORY.createStatement(createHttpUri("a"), createHttpUri("b"), createHttpUri("c"));
        preprocessor.setDefaultContext(createHttpUri("g1"));
        preprocessor.handleStatement(statement1);

        Statement statement2 = VALUE_FACTORY.createStatement(createHttpUri("x"), createHttpUri("y"), createHttpUri("z"));
        preprocessor.setDefaultContext(createHttpUri("g2"));
        preprocessor.handleStatement(statement2);

        preprocessor.endRDF();

        // Assert
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), contextAwareStatementIsEqual(createHttpStatement("a", "b", "c", "g1")));
        assertThat(result.get(1), contextAwareStatementIsEqual(createHttpStatement("x", "y", "z", "g2")));
    }

    @Test
    public void filtersUnmappedSubjectsWhenOutputMappedSubjectsOnlyIsTrue() throws Exception {
        // Arrange
        ArrayList<Statement> statements = new ArrayList<Statement>();
        statements.add(createHttpStatement("s1", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s3", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s2", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s4", "p1", "o1", "g1"));

        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(ImmutableSet.of(
                createHttpUri("s1").toString(), createHttpUri("s2").toString()));
        uriMapping.addLink(createHttpUri("s1").toString(), createHttpUri("sx").toString());
        uriMapping.addLink(createHttpUri("s2").toString(), createHttpUri("sy").toString());

        // Act
        ArrayList<Statement> result = new ArrayList<Statement>();
        ExternalSortingInputLoaderPreprocessor preprocessor = new ExternalSortingInputLoaderPreprocessor(
                uriMapping,
                new StatementCollector(result),
                Long.MAX_VALUE,
                VALUE_FACTORY,
                COMPARATOR,
                true);
        preprocessor.startRDF();
        for (Statement statement : statements) {
            preprocessor.handleStatement(statement);
        }
        preprocessor.endRDF();

        // Assert
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g1")));
        assertThat(result.get(1), contextAwareStatementIsEqual(createHttpStatement("s2", "p1", "o1", "g1")));
    }

    private ArrayList<Statement> collectResultsFromPreprocessor(ArrayList<Statement> statements, URIMappingIterable uriMapping, long memoryLimit)
            throws RDFHandlerException {

        ArrayList<Statement> result = new ArrayList<Statement>();
        ExternalSortingInputLoaderPreprocessor preprocessor = new ExternalSortingInputLoaderPreprocessor(
                uriMapping,
                new StatementCollector(result),
                memoryLimit,
                VALUE_FACTORY,
                COMPARATOR,
                false);
        preprocessor.startRDF();
        for (Statement statement : statements) {
            preprocessor.handleStatement(statement);
        }
        preprocessor.endRDF();
        return result;
    }
}