package cz.cuni.mff.odcleanstore.fusiontool;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.SpogComparator;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigReader;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterableImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.Rio;

import java.io.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class ODCSFusionToolExecutorRunnerIntegrationTest {
    public static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();

    @Rule
    public TemporaryFolder testDir = new TemporaryFolder();
    private File resourceDir;

    @Before
    public void setUp() throws Exception {
        resourceDir = new File(getClass().getResource(".").toURI());
    }

    @Test
    public void testRunWithTransitiveSeedResources() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-seedTransitive.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-seedTransitive.trig"));
    }

    @Test
    public void testRunWithTransitiveSeedResourcesAndFileCache() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-seedTransitive-fileCache.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-seedTransitive.trig"));
    }

    @Test
    public void testRunWithTransitiveSeedResourcesAndGzippedInput() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-seedTransitive-gz.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-seedTransitive.trig"));
    }

    @Test
    public void testRunWithLocalCopyProcessing() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-localCopyProcessing.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-localCopyProcessing.trig"));
    }

    @Test
    public void testRunWithLocalCopyProcessingAndGzippedInput() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-localCopyProcessing-gz.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-localCopyProcessing.trig"));
    }

    private void runTestWithConfig(ConfigImpl config, File expectedCanonicalUriFile, File expectedSameAsFile, File expectedOutputFile) throws ODCSFusionToolException, IOException, ConflictResolutionException, RDFParseException {
        File tempDirectory = new File(testDir.getRoot(), "temp");
        tempDirectory.getAbsoluteFile().mkdirs();
        config.setTempDirectory(tempDirectory);

        Map<String, String> dataSourceParams = config.getDataSources().get(0).getParams();
        dataSourceParams.put(ConfigParameters.DATA_SOURCE_FILE_PATH,
                convertToResourceFile(dataSourceParams.get(ConfigParameters.DATA_SOURCE_FILE_PATH)).getAbsolutePath());
        Map<String, String> sameAsSourceParams = config.getSameAsSources().get(0).getParams();
        sameAsSourceParams.put(ConfigParameters.DATA_SOURCE_FILE_PATH,
                convertToResourceFile(sameAsSourceParams.get(ConfigParameters.DATA_SOURCE_FILE_PATH)).getAbsolutePath());
        config.setCanonicalURIsInputFile(convertToResourceFile(config.getCanonicalURIsInputFile().getPath()));

        File canonicalUrisOutputFile = convertToTempFile(config.getCanonicalURIsOutputFile().getPath());
        config.setCanonicalURIsOutputFile(canonicalUrisOutputFile);
        Map<String, String> outputParams = config.getOutputs().get(0).getParams();
        File outputFile = convertToTempFile(outputParams.get(ConfigParameters.OUTPUT_PATH));
        outputParams.put(ConfigParameters.OUTPUT_PATH, outputFile.getAbsolutePath());
        File sameAsFile = convertToTempFile(outputParams.get(ConfigParameters.OUTPUT_SAME_AS_FILE));
        outputParams.put(ConfigParameters.OUTPUT_SAME_AS_FILE, sameAsFile.getAbsolutePath());

        // Act
        ODCSFusionToolExecutorRunner runner = new ODCSFusionToolExecutorRunner(config);
        runner.runFusionTool();

        // Assert - canonical URIs
        Set<String> canonicalUris = parseCanonicalUris(canonicalUrisOutputFile);
        Set<String> expectedCanonicalUris = parseCanonicalUris(expectedCanonicalUriFile);
        assertThat(canonicalUris, equalTo(expectedCanonicalUris));

        // Assert - sameAs
        Set<Statement> sameAs = parseStatements(sameAsFile);
        URIMappingIterable uriMapping = createUriMapping(sameAs, canonicalUris);
        Set<Statement> expectedSameAs = parseStatements(expectedSameAsFile);
        URIMappingIterable expectedUriMapping = createUriMapping(expectedSameAs, canonicalUris);
        for (String uri : expectedUriMapping) {
            URI canonicalUri = uriMapping.mapURI(VALUE_FACTORY.createURI(uri));
            URI expectedCanonicalUri = expectedUriMapping.mapURI(VALUE_FACTORY.createURI(uri));
            assertThat(canonicalUri, equalTo(expectedCanonicalUri));
        }

        // Assert - output
        Statement[] dataOutput = parseStatements(outputFile).toArray(new Statement[0]);
        Statement[] expectedOutput = parseStatements(expectedOutputFile).toArray(new Statement[0]);
        //assertThat(dataOutput, equalTo(expectedOutput));
        assertThat(dataOutput.length, equalTo(expectedOutput.length));
        BiMap<BNode, BNode> bNodeMap = HashBiMap.create();
        for (int i = 0; i < dataOutput.length; i++) {
            Statement actualStatement = dataOutput[i];
            Statement expectedStatement = tryMatchBNodes(expectedOutput[i], actualStatement, bNodeMap);

            assertThat(actualStatement, equalTo(expectedStatement));
            assertThat(actualStatement.getContext(), equalTo(expectedStatement.getContext()));
        }
    }

    private Statement tryMatchBNodes(Statement expectedStatement, Statement actualStatement, BiMap<BNode, BNode> bNodeMap) {
        if (expectedStatement.getSubject() instanceof BNode
                || expectedStatement.getObject() instanceof BNode
                || expectedStatement.getContext() instanceof BNode) {
            return VALUE_FACTORY.createStatement(
                    (Resource) tryMatchBNode(expectedStatement.getSubject(), actualStatement.getSubject(), bNodeMap),
                    expectedStatement.getPredicate(),
                    tryMatchBNode(expectedStatement.getObject(), actualStatement.getObject(), bNodeMap),
                    (Resource) tryMatchBNode(expectedStatement.getContext(), actualStatement.getContext(), bNodeMap));
        } else {
            return expectedStatement;
        }
    }

    private Value tryMatchBNode(Value expected, Value actual, BiMap<BNode, BNode> bNodeMap) {
        if (!(expected instanceof BNode)) {
            // map only BNodes
            return expected;
        } else if (bNodeMap.containsKey(expected)) {
            // this BNode already has a mapping, use it consistently
            return bNodeMap.get(expected);
        } else if (actual instanceof BNode && !bNodeMap.inverse().containsKey(actual)) {
            // actual value is also a bnode and there is no other expected node mapped to it
            bNodeMap.put((BNode) expected, (BNode) actual);
            return actual;
        } else {
            // cannot map to actual value
            return expected;
        }
    }

    private Set<String> parseCanonicalUris(File file) throws IOException {
        Set<String> result = new HashSet<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), "UTF-8"));
        String line = reader.readLine();
        while (line != null) {
            result.add(line);
            line = reader.readLine();
        }
        reader.close();
        return result;
    }

    private URIMappingIterable createUriMapping(Set<Statement> sameAs, Set<String> canonicalUris) {
        URIMappingIterableImpl uriMapping = new URIMappingIterableImpl(canonicalUris);
        for (Statement statement : sameAs) {
            uriMapping.addLink(statement.getSubject().stringValue(), statement.getObject().stringValue());
        }
        return uriMapping;
    }

    private File convertToResourceFile(String path) {
        File file = new File(path);
        return new File(resourceDir, file.getName());
    }

    private File convertToTempFile(String path) {
        return new File(testDir.getRoot(), path);
    }

    private TreeSet<Statement> parseStatements(File file) throws IOException, RDFParseException {
        FileInputStream inputStream = new FileInputStream(file);
        RDFFormat rdfFormat = Rio.getParserFormatForFileName(file.getName());
        Model model = Rio.parse(
                inputStream,
                file.toURI().toString(),
                rdfFormat);
        inputStream.close();
        TreeSet<Statement> result = Sets.newTreeSet(new SpogComparator());
        result.addAll(model);
        return result;
    }
}