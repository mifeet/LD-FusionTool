package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesLoader;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesRepositoryLoader;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSourceImpl;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.memory.MemoryStore;

import java.util.*;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.LDFusionToolTestUtils.createHttpStatement;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllTriplesRepositoryLoaderTest {
    public static final SparqlRestrictionImpl EMPTY_SPARQL_RESTRICTION = new SparqlRestrictionImpl("", "338ae1bdf9_x");

    @Test
    public void loadsAllTriplesWhenNumberOfStatementsIsNotDivisibleByMaxResultSize() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2"),
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4"),
                createHttpStatement("s5", "p", "o", "g5")
        );
        DataSource dataSource = createDataSource(statements, 2);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

        // Assert
        assertThat(result, is(statements));
        dataSource.getRepository().shutDown();
    }

    @Test
    public void loadsAllTriplesWhenNumberOfStatementsIsDivisibleByMaxResultSize() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2"),
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4")
        );
        DataSource dataSource = createDataSource(statements, 2);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

        // Assert
        assertThat(result, is(statements));
        dataSource.getRepository().shutDown();
    }

    @Test
    public void returnsEmptyResultWhenNoMatchingTriplesExist() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of();
        DataSource dataSource = createDataSource(statements, 2);

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

        // Assert
        assertThat(result.size(), equalTo(0));
        dataSource.getRepository().shutDown();
    }

    @Test
    public void limitsResultsWhenNamedGraphRestrictionGiven() throws Exception {
        // Arrange
        Statement statement1 = createHttpStatement("s1", "p", "o", "g1");
        Statement statement2 = createHttpStatement("s2", "p", "o", "g2");
        List<Statement> statements = ImmutableList.of(statement1, statement2);

        SparqlRestriction namedGraphRestriction = new SparqlRestrictionImpl(
                "FILTER(?gg = <" + statement1.getContext().stringValue() + ">)",
                "gg");
        DataSource dataSource = createDataSource(statements, namedGraphRestriction, new HashMap<String, String>(), 100, "test");

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

        // Assert
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(), contextAwareStatementIsEqual(statement1));
        dataSource.getRepository().shutDown();
    }

    @Test
    public void callsStartRDFAndEndRDFOnGivenHandler() throws Exception {
        // Arrange
        RDFHandler rdfHandler = mock(RDFHandler.class);
        Collection<Statement> statements = ImmutableList.of(
                createHttpStatement("s1", "p", "o", "g1")
        );
        DataSource dataSource = createDataSource(statements, 2);

        // Act
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples((rdfHandler));
        loader.close();

        // Assert
        Mockito.verify(rdfHandler).startRDF();
        Mockito.verify(rdfHandler).endRDF();
    }

    @Test
    public void usesPrefixesWhenPrefixesGiven() throws Exception {
        // Arrange
        Statement statement1 = createHttpStatement("s1", "p", "o", "example1.com/g1");
        Statement statement2 = createHttpStatement("s2", "p", "o", "example2.com/g2");
        List<Statement> statements = ImmutableList.of(statement1, statement2);

        SparqlRestriction namedGraphRestriction = new SparqlRestrictionImpl("FILTER(?gg = ex1:g1)", "gg");
        HashMap<String, String> prefixes = new HashMap<>();
        prefixes.put("ex1", "http://example1.com/");
        prefixes.put("ex2", "http://example2.com/");
        DataSource dataSource = createDataSource(statements, namedGraphRestriction, prefixes, 100, "test");

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesRepositoryLoader loader = new AllTriplesRepositoryLoader(dataSource);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

        // Assert
        assertThat(result.size(), equalTo(1));
        assertThat(result.iterator().next(), contextAwareStatementIsEqual(statement1));
        dataSource.getRepository().shutDown();
    }

    @Test
    public void returnsNonNullDefaultContext() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of();
        DataSource dataSource = createDataSource(statements, 2);
        assertThat(dataSource.getParams().get(ConfigParameters.DATA_SOURCE_FILE_BASE_URI), nullValue());

        // Act
        AllTriplesLoader loader = new AllTriplesRepositoryLoader(dataSource);
        URI defaultContext = loader.getDefaultContext();
        loader.close();

        // Assert
        assertTrue(ODCSUtils.isValidIRI(defaultContext.stringValue()));
    }

    @Ignore("Until retry timout is given in Configuration, ignore so that the tests aren't too slow")
    @Test
    public void retriesQueryOnError() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableSet.of(
                createHttpStatement("s1", "p", "o", "g1"),
                createHttpStatement("s2", "p", "o", "g2"),
                createHttpStatement("s3", "p", "o", "g3"),
                createHttpStatement("s4", "p", "o", "g4"),
                createHttpStatement("s5", "p", "o", "g5")
        );
        DataSource dataSource = createDataSource(statements, 2);
        final SailRepository repository = (SailRepository) dataSource.getRepository();
        RepositoryConnection mockRepositoryConnection = mock(RepositoryConnection.class);
        Answer<TupleQuery> answer = new Answer<TupleQuery>() {
            @Override
            public TupleQuery answer(InvocationOnMock invocation) throws Throwable {
                Object[] arguments = invocation.getArguments();
                return repository.getConnection().prepareTupleQuery((QueryLanguage) arguments[0], (String) arguments[1]);
            }
        };
        when(mockRepositoryConnection.prepareTupleQuery(any(QueryLanguage.class), anyString()))
                .thenAnswer(answer)
                .thenThrow(new RepositoryException())
                .thenThrow(new RepositoryException())
                .thenAnswer(answer);
        Repository mockRepository = mock(Repository.class);
        when(mockRepository.getValueFactory()).thenReturn(repository.getValueFactory());
        when(mockRepository.getConnection()).thenReturn(mockRepositoryConnection);
        DataSource mockDataSource = new DataSourceImpl(
                mockRepository,
                dataSource.getPrefixes(),
                dataSource.getName(),
                dataSource.getType(),
                dataSource.getParams(),
                dataSource.getNamedGraphRestriction());

        // Act
        Collection<Statement> result = new HashSet<>();
        AllTriplesLoader loader = new AllTriplesRepositoryLoader(mockDataSource);
        long startTime = System.currentTimeMillis();
        loader.loadAllTriples(new StatementCollector(result));
        long endTime = System.currentTimeMillis();
        loader.close();

        // Assert
        assertThat(result, is(statements));
        assertThat(endTime - startTime, Matchers.greaterThanOrEqualTo(2L * LDFTConfigConstants.REPOSITORY_RETRY_INTERVAL));
        dataSource.getRepository().shutDown();
    }

    private DataSource createDataSource(Collection<Statement> statements, int maxSparqlResultRows) throws RepositoryException {
        return createDataSource(
                statements,
                EMPTY_SPARQL_RESTRICTION,
                new HashMap<String, String>(),
                maxSparqlResultRows,
                "test");
    }

    private DataSource createDataSource(
            Collection<Statement> statements,
            SparqlRestriction namedGraphRestriction,
            Map<String, String> prefixes,
            int maxSparqlResultRows,
            String name)
            throws RepositoryException {
        Repository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        RepositoryConnection connection = repository.getConnection();
        connection.add(statements);
        connection.close();
        Map<String, String> params = ImmutableMap.of(ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS, Integer.toString(maxSparqlResultRows));
        return new DataSourceImpl(repository, prefixes, name, EnumDataSourceType.SPARQL, params, namedGraphRestriction);
    }
}