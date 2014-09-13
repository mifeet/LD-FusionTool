package cz.cuni.mff.odcleanstore.fusiontool.io;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.StatementCollector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.LDFusionToolTestUtils.createHttpStatement;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.LDFusionToolTestUtils.createHttpUri;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;

public class RdfFileLoaderTest {
    private static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();

    private AtomicInteger testFileCounter = new AtomicInteger(0);

    private List<Statement> testInput1 = ImmutableList.of(
            createHttpStatement("sa", "pa", "oa", "g1"),
            createHttpStatement("sb", "pb", "ob", "g1"),
            createHttpStatement("sa", "p1", "oa", "ga"),
            VALUE_FACTORY.createStatement(createHttpUri("x"), createHttpUri("y"), VALUE_FACTORY.createLiteral("11")));

    @Test
    public void loadsAllStatementsWhenTrigFileGiven() throws Exception {
        // Arrange
        DataSourceConfig dataSource = createFileDataSource(testInput1, EnumSerializationFormat.TRIG);

        // Act
        List<Statement> result = new ArrayList<Statement>();
        RdfFileLoader loader = new RdfFileLoader(dataSource, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.read(new StatementCollector(result));

        // Assert
        assertThat(result.size(), equalTo(testInput1.size()));
        for (int i = 0; i < testInput1.size(); i++) {
            // compare including named graphs
            assertThat(result.get(i), contextAwareStatementIsEqual(testInput1.get(i)));
        }
    }

    @Test
    public void loadsAllStatementsWhenRdfXmlFileGiven() throws Exception {
        // ArrangeList<Statement> result = new ArrayList<Statement>();
        DataSourceConfig dataSource = createFileDataSource(testInput1, EnumSerializationFormat.RDF_XML);

        // Act
        List<Statement> result = new ArrayList<Statement>();
        RdfFileLoader loader = new RdfFileLoader(dataSource, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.read(new StatementCollector(result));

        // Assert
        assertThat(result.size(), equalTo(testInput1.size()));
        for (int i = 0; i < testInput1.size(); i++) {
            // compare including named graphs
            assertThat(result.get(i), equalTo(testInput1.get(i))); // intentionally do not test context
        }
    }

    @Test
    public void returnsEmptyResultWhenInputFileEmpty() throws Exception {
        // Arrange
        Collection<Statement> statements = ImmutableList.of();
        DataSourceConfig dataSource = createFileDataSource(statements, EnumSerializationFormat.RDF_XML);

        // Act
        RdfFileLoader loader = new RdfFileLoader(dataSource, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        List<Statement> result = new ArrayList<Statement>(0);
        loader.read(new StatementCollector(result));

        // Assert
        assertThat(result.size(), equalTo(0));
    }

    @Test
    public void callsStartRDFAndEndRDFOnGivenHandler() throws Exception {
        // Arrange
        RDFHandler rdfHandler = Mockito.mock(RDFHandler.class);
        Collection<Statement> statements = ImmutableList.of(
                createHttpStatement("s1", "p", "o", "g1")
        );
        DataSourceConfig dataSource = createFileDataSource(statements, EnumSerializationFormat.RDF_XML);

        // Act
        RdfFileLoader loader = new RdfFileLoader(dataSource, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.read((rdfHandler));

        // Assert
        Mockito.verify(rdfHandler).startRDF();
        Mockito.verify(rdfHandler).endRDF();
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionWhenDataSourceHasNotTypeFile() throws Exception {
        // Arrange
        DataSourceConfig dataSourceConfig = new DataSourceConfigImpl(EnumDataSourceType.SPARQL, "");

        // Act
        RdfFileLoader loader = new RdfFileLoader(dataSourceConfig, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.read(new StatementCollector());
    }

    @Test(expected = RDFHandlerException.class)
    public void throwsRDFHandlerExceptionWhenHandlerFails() throws Exception {
        // Arrange
        RDFHandler rdfHandler = Mockito.mock(RDFHandler.class);
        doThrow(new RDFHandlerException("")).when(rdfHandler).handleStatement(any(Statement.class));
        Collection<Statement> statements = ImmutableList.of(
                createHttpStatement("s1", "p", "o", "g1")
        );
        DataSourceConfig dataSource = createFileDataSource(statements, EnumSerializationFormat.RDF_XML);

        // Act
        RdfFileLoader loader = new RdfFileLoader(dataSource, LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.read((rdfHandler));
    }

    private DataSourceConfig createFileDataSource(Collection<Statement> statements, EnumSerializationFormat format) throws IOException, RDFHandlerException {
        DataSourceConfigImpl result = new DataSourceConfigImpl(
                EnumDataSourceType.FILE,
                "test-input-file" + testFileCounter.getAndIncrement() + ".input");
        File inputFile = createInputFile(statements, format.toSesameFormat());
        result.getParams().put(ConfigParameters.DATA_SOURCE_FILE_PATH, inputFile.getAbsolutePath());
        result.getParams().put(ConfigParameters.DATA_SOURCE_FILE_FORMAT, format.name());
        return result;
    }

    private File createInputFile(Collection<Statement> statements, RDFFormat format) throws IOException, RDFHandlerException {
        File inputFile = testDir.newFile();
        FileOutputStream outputStream = new FileOutputStream(inputFile);
        RDFWriter rdfWriter = Rio.createWriter(format, outputStream);
        rdfWriter.startRDF();
        rdfWriter.handleComment("Test input file");
        for (Statement statement : statements) {
            rdfWriter.handleStatement(statement);
        }
        rdfWriter.endRDF();
        outputStream.close();
        return inputFile;
    }
}