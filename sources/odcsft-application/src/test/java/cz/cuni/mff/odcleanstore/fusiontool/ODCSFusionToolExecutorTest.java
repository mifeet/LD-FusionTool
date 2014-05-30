package cz.cuni.mff.odcleanstore.fusiontool;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.TestConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.TestInputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.TestRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.model.Statement;

import java.util.Collection;
import java.util.List;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpStatement;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ODCSFusionToolExecutorTest {
    @Test
    public void processesAllInputStatements() throws Exception {
        // Arrange
        ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor();
        TestInputLoader inputLoader = new TestInputLoader(ImmutableList.of(
                //(Collection<Statement>) ImmutableList.<Statement>of(),
                (Collection<Statement>) ImmutableList.of(
                        createHttpStatement("s1", "p1", "o1", "g1"),
                        createHttpStatement("s1", "p1", "o1", "g2")),


                ImmutableList.of(
                        createHttpStatement("s2", "p2", "o1", "g3"),
                        createHttpStatement("s2", "p2", "o2", "g3"))
        ));
        TestRDFWriter rdfWriter = new TestRDFWriter();

        // Act
        executor.execute(inputLoader, rdfWriter, new TestConflictResolver());

        // Assert
        List<ResolvedStatement> resolvedStatements = rdfWriter.getCollectedResolvedStatements();
        assertThat(resolvedStatements.get(0).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g1")));
        assertThat(resolvedStatements.get(1).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g2")));
        assertThat(resolvedStatements.get(2).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s2", "p2", "o1", "g3")));
        assertThat(resolvedStatements.get(3).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s2", "p2", "o2", "g3")));
    }

    @Test
    public void respectsMaxOutputTriples() throws Exception {
        // Arrange
        long maxOutputTriples = 5;
        ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor(false, maxOutputTriples, false);
        TestInputLoader inputLoader = new TestInputLoader(ImmutableList.<Collection<Statement>>of(
                ImmutableList.of(
                        createHttpStatement("s1", "p1", "o1", "g1"),
                        createHttpStatement("s1", "p1", "o1", "g2")),

                ImmutableList.of(
                        createHttpStatement("s2", "p1", "o1", "g1"),
                        createHttpStatement("s2", "p1", "o1", "g2")),

                ImmutableList.of(
                        createHttpStatement("s3", "p1", "o1", "g1"),
                        createHttpStatement("s3", "p1", "o1", "g2"))
        ));
        TestRDFWriter rdfWriter = new TestRDFWriter();

        // Act
        executor.execute(inputLoader, rdfWriter, new TestConflictResolver());

        // Assert
        List<ResolvedStatement> resolvedStatements = rdfWriter.getCollectedResolvedStatements();
        Assert.assertTrue(resolvedStatements.size() <= maxOutputTriples);
        Assert.assertTrue(resolvedStatements.size() >= maxOutputTriples % 2); // whole input clusters are processed
    }

    @Test
    public void suppliesAllQuadsInClusterToConflictResolver() throws Exception {
        // Arrange
        long maxOutputTriples = 5;
        ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor(false, maxOutputTriples, false);
        ImmutableList<Collection<Statement>> inputStatements = ImmutableList.<Collection<Statement>>of(
                ImmutableList.of(
                        createHttpStatement("s1", "p1", "o1", "g1"),
                        createHttpStatement("s1", "p1", "o1", "g2"),
                        createHttpStatement("s1", "p1", "o1", "g3")),

                ImmutableList.of(
                        createHttpStatement("s2", "p1", "o1", "g1")),

                ImmutableList.of(
                        createHttpStatement("s3", "p1", "o1", "g1"),
                        createHttpStatement("s3", "p1", "o1", "g2"))
        );
        TestInputLoader inputLoader = new TestInputLoader(inputStatements);
        CloseableRDFWriter rdfWriter = Mockito.mock(CloseableRDFWriter.class);
        TestConflictResolver conflictResolver = new TestConflictResolver();

        // Act
        executor.execute(inputLoader, rdfWriter, conflictResolver);

        // Assert
        assertThat(conflictResolver.getCollectedStatements().size(), equalTo(inputStatements.size()));
        for (int i = 0; i < inputStatements.size(); i++) {
            assertThat(conflictResolver.getCollectedStatements().get(i), equalTo(inputStatements.get(i)));
        }
    }

    @Test
    public void updatesInputLoaderWithResolvedStatements() throws Exception {
        // Arrange
        long maxOutputTriples = 5;
        ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor(false, maxOutputTriples, false);
        ImmutableList<Collection<Statement>> inputStatements = ImmutableList.<Collection<Statement>>of(
                ImmutableList.of(
                        createHttpStatement("s1", "p1", "o1", "g1"),
                        createHttpStatement("s1", "p1", "o1", "g2"),
                        createHttpStatement("s1", "p1", "o1", "g3")),

                ImmutableList.of(
                        createHttpStatement("s2", "p1", "o1", "g1")),

                ImmutableList.of(
                        createHttpStatement("s3", "p1", "o1", "g1"),
                        createHttpStatement("s3", "p1", "o1", "g2"))
        );
        TestInputLoader inputLoader = new TestInputLoader(inputStatements);
        TestRDFWriter rdfWriter = new TestRDFWriter();
        TestConflictResolver conflictResolver = new TestConflictResolver();

        // Act
        executor.execute(inputLoader, rdfWriter, conflictResolver);

        // Assert
        assertThat(inputLoader.getCollectedResolvedStatements(), equalTo(rdfWriter.collectedResolvedStatements));
    }

    @Test
    public void processesAllInputStatementsWhenHasVirtuosoSource() throws Exception {
        // Arrange
        ODCSFusionToolExecutor executor = new ODCSFusionToolExecutor(true, Long.MAX_VALUE, true);
        TestInputLoader inputLoader = new TestInputLoader(ImmutableList.of(
                (Collection<Statement>) ImmutableList.<Statement>of(),

                ImmutableList.of(
                        createHttpStatement("s1", "p1", "o1", "g1"),
                        createHttpStatement("s1", "p1", "o1", "g2")),


                ImmutableList.of(
                        createHttpStatement("s2", "p2", "o1", "g3"),
                        createHttpStatement("s2", "p2", "o2", "g3"))
        ));
        TestRDFWriter rdfWriter = new TestRDFWriter();

        // Act
        executor.execute(inputLoader, rdfWriter, new TestConflictResolver());

        // Assert
        List<ResolvedStatement> resolvedStatements = rdfWriter.getCollectedResolvedStatements();
        assertThat(resolvedStatements.get(0).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g1")));
        assertThat(resolvedStatements.get(1).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s1", "p1", "o1", "g2")));
        assertThat(resolvedStatements.get(2).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s2", "p2", "o1", "g3")));
        assertThat(resolvedStatements.get(3).getStatement(), contextAwareStatementIsEqual(createHttpStatement("s2", "p2", "o2", "g3")));
    }
}