package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.*;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.data.AllTriplesFileLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.StatementCollector;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpStatement;
import static cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils.createHttpUri;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AllTriplesFileLoaderTest {
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
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSource, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

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
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSource, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

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
        List<Statement> result = new ArrayList<Statement>(0);
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSource, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.loadAllTriples(new StatementCollector(result));
        loader.close();

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
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSource, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        loader.loadAllTriples((rdfHandler));
        loader.close();

        // Assert
        Mockito.verify(rdfHandler).startRDF();
        Mockito.verify(rdfHandler).endRDF();
    }

    @Test
    public void returnsNonNullDefaultContext() throws Exception {
        // Arrange
        Collection<Statement> statements = Collections.emptySet();
        DataSourceConfig dataSource = createFileDataSource(statements, EnumSerializationFormat.RDF_XML);
        dataSource.getParams().remove(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);

        // Act
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSource, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        URI defaultContext = loader.getDefaultContext();

        // Assert
        assertTrue(ODCSUtils.isValidIRI(defaultContext.stringValue()));
    }

    @Test
    public void returnsBaseUriAsDefaultContextWhenGiven() throws Exception {
        // Arrange
        String baseUri = "http://baseuri";
        Collection<Statement> statements = Collections.emptySet();
        DataSourceConfigImpl dataSourceConfig = new DataSourceConfigImpl(
                EnumDataSourceType.FILE,
                "test-input-file" + testFileCounter.getAndIncrement() + ".input");
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_FILE_BASE_URI, baseUri);
        File inputFile = createInputFile(statements, EnumSerializationFormat.RDF_XML.toSesameFormat());
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_FILE_PATH, inputFile.getAbsolutePath());
        dataSourceConfig.getParams().put(ConfigParameters.DATA_SOURCE_FILE_FORMAT, EnumSerializationFormat.RDF_XML.name());

        // Act
        AllTriplesFileLoader loader = new AllTriplesFileLoader(dataSourceConfig, ConfigConstants.DEFAULT_FILE_PARSER_CONFIG);
        URI defaultContext = loader.getDefaultContext();

        // Assert
        assertThat(defaultContext.toString(), equalTo(baseUri));
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