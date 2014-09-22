package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.testutil.LDFusionToolTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultContextRDFInserterTest {

    public static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();
    private URI defaultContext;

    @Before
    public void setUp() throws Exception {
        defaultContext = LDFusionToolTestUtils.createHttpUri("defaultContext");
    }

    @Test
    public void insertsStatementWithoutContextToDefaultContext() throws Exception {
        // Arrange
        Repository repository = initRepository();
        RepositoryConnection connection = repository.getConnection();
        Statement statement = LDFusionToolTestUtils.createHttpStatement("a", "b", "c", null);

        // Act
        DefaultContextRDFInserter inserter = new DefaultContextRDFInserter(connection, defaultContext);
        inserter.startRDF();
        inserter.handleStatement(statement);
        inserter.endRDF();

        // Assert
        assertTrue(connection.hasStatement(
                statement.getSubject(),
                statement.getPredicate(),
                statement.getObject(),
                false,
                defaultContext));

        connection.close();
        repository.shutDown();
    }

    @Test
    public void insertsStatementsWithContextToTheirContext() throws Exception {
        // Arrange
        Repository repository = initRepository();
        RepositoryConnection connection = repository.getConnection();
        URI statementContext = LDFusionToolTestUtils.createHttpUri("d");
        Statement statement = LDFusionToolTestUtils.createHttpStatement("a", "b", "c", "d");

        // Act
        DefaultContextRDFInserter inserter = new DefaultContextRDFInserter(connection, defaultContext);
        inserter.startRDF();
        inserter.handleStatement(statement);
        inserter.endRDF();

        // Assert
        assertTrue(connection.hasStatement(
                statement.getSubject(),
                statement.getPredicate(),
                statement.getObject(),
                false,
                statementContext));

        connection.close();
        repository.shutDown();
    }

    @Test
    public void preservesBNodes() throws Exception {
        // Arrange
        Repository repository = initRepository();
        RepositoryConnection connection = repository.getConnection();
        Statement statement = VALUE_FACTORY.createStatement(
                VALUE_FACTORY.createBNode("bnode1"),
                VALUE_FACTORY.createURI("http://p"),
                VALUE_FACTORY.createBNode("bnode2")
        );

        // Act
        DefaultContextRDFInserter inserter = new DefaultContextRDFInserter(connection, defaultContext);
        inserter.startRDF();
        inserter.handleStatement(statement);
        inserter.endRDF();

        // Assert
        RepositoryResult<Statement> statements = connection.getStatements(null, null, null, false);
        assertTrue(statements.hasNext());
        Statement resultStatement = statements.next();
        assertThat(resultStatement.getSubject(), equalTo(statement.getSubject()));
        assertThat(resultStatement.getObject(), equalTo(statement.getObject()));
        assertThat(resultStatement.getContext(), equalTo((Resource) defaultContext));

        connection.close();
        repository.shutDown();
    }

    private SailRepository initRepository() throws RepositoryException {
        SailRepository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        return repository;
    }


}