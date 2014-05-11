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
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
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
import java.util.*;

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
    public void testRunWithNonTransitiveSeedResources() throws Exception {
        // Arrange
        File configFile = new File(resourceDir, "config-seedNonTransitive.xml");
        ConfigImpl config = (ConfigImpl) ConfigReader.parseConfigXml(configFile);

        runTestWithConfig(
                config,
                new File(resourceDir, "canonical.txt"),
                new File(resourceDir, "sameAs.ttl"),
                new File(resourceDir, "expectedOutput-seedNonTransitive.trig"));
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
        Model actualModel = parseStatements(outputFile);
        Statement[] actualOutput = normalizeActualOutput(actualModel);
        Statement[] expectedOutput = normalizeToMatchActualOutput(parseStatements(expectedOutputFile), actualOutput);
        //assertThat(dataOutput, equalTo(expectedOutput));
        int minCommonLength = Math.min(actualOutput.length, expectedOutput.length);
        for (int i = 0; i < minCommonLength; i++) {
            Statement actualStatement = actualOutput[i];
            Statement expectedStatement = expectedOutput[i];

            assertThat(actualStatement, equalTo(expectedStatement));
            assertThat(actualStatement.getContext(), equalTo(expectedStatement.getContext()));
        }
        assertThat(actualOutput.length, equalTo(expectedOutput.length));
    }

    /**
     * Return SPOG-sorted statements.
     */
    private Statement[] normalizeActualOutput(Model model) {
        TreeSet<Statement> statements = Sets.newTreeSet(new SpogComparator());
        statements.addAll(model);
        return statements.toArray(new Statement[statements.size()]);
    }

    /**
     * Necessary to SPOG-sort statements, map blank nodes, which have different identifiers in different files,
     * and named graphs, which depend on the order CR was executed in.
     */
    private Statement[] normalizeToMatchActualOutput(Model expectedOutput, Statement[] actualOutput) {
        // Map named graphs
        Statement[] expectedStatements = expectedOutput.toArray(new Statement[expectedOutput.size()]);
        Resource metadataContext = getMetadataContext(actualOutput);
        if (metadataContext != null) {
            Map<Statement, Resource> actualStatementsToContext = new HashMap<Statement, Resource>();
            for (Statement statement : actualOutput) {
                actualStatementsToContext.put(setContext(statement, null), statement.getContext());
            }
            Map<Resource, Resource> contextsMapping = new HashMap<Resource, Resource>();
            for (Statement statement : expectedStatements) {
                Resource matchingActualContext = actualStatementsToContext.get(setContext(statement, null));
                if (!metadataContext.equals(statement.getContext()) && matchingActualContext != null) {
                    contextsMapping.put(statement.getContext(), matchingActualContext);
                }
            }
            for (int i = 0; i < expectedStatements.length; i++) {
                if (contextsMapping.containsKey(expectedStatements[i].getContext())) {
                    expectedStatements[i] = setContext(expectedStatements[i], contextsMapping.get(expectedStatements[i].getContext()));
                } else if (contextsMapping.containsKey(expectedStatements[i].getSubject())) {
                    expectedStatements[i] = setSubject(expectedStatements[i], contextsMapping.get(expectedStatements[i].getSubject()));
                }
            }
        }

        // SPOG-sort
        TreeSet<Statement> expectedStatementsTreeSet = Sets.newTreeSet(new SpogComparator());
        expectedStatementsTreeSet.addAll(Arrays.asList(expectedStatements));
        Statement[] result = expectedStatementsTreeSet.toArray(new Statement[expectedStatementsTreeSet.size()]);

        // Map blank nodes
        BiMap<BNode, BNode> bNodeMap = HashBiMap.create();
        int minCommonLength = Math.min(expectedStatementsTreeSet.size(), actualOutput.length);
        for (int i = 0; i < minCommonLength; i++) {
            result[i] = tryMatchBNodes(result[i], actualOutput[i], bNodeMap);
        }
        return result;
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

    private Model parseStatements(File file) throws IOException, RDFParseException {
        FileInputStream inputStream = new FileInputStream(file);
        RDFFormat rdfFormat = Rio.getParserFormatForFileName(file.getName());
        Model model = Rio.parse(
                inputStream,
                file.toURI().toString(),
                rdfFormat);
        inputStream.close();
        return model;
    }

    private Resource getMetadataContext(Statement[] actualOutput) {
        for (Statement statement : actualOutput) {
            if (ODCS.SOURCE_GRAPH.equals(statement.getPredicate())) {
                return statement.getContext();
            }
        }
        return null;
    }

    private Statement setContext(Statement statement, Resource context) {
        return VALUE_FACTORY.createStatement(
                statement.getSubject(),
                statement.getPredicate(),
                statement.getObject(),
                context);
    }

    private Statement setSubject(Statement statement, Resource subject) {
        return VALUE_FACTORY.createStatement(
                subject,
                statement.getPredicate(),
                statement.getObject(),
                statement.getContext());
    }
}