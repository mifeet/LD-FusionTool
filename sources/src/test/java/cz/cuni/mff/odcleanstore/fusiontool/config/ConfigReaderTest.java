package cz.cuni.mff.odcleanstore.fusiontool.config;

import com.google.common.collect.ImmutableMap;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumCardinality;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.InvalidInputException;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ConfigReaderTest {
    @Test
     public void parsesMinimalConfigFile() throws Exception {
        // Arrange
        File configFile = getResourceFile("/config/sample-config-minimal.xml");

        // Act
        Config config = ConfigReader.parseConfigXml(configFile);

        // Assert
        assertThat(config.getCanonicalURIsInputFile(), nullValue());
        assertThat(config.getCanonicalURIsOutputFile(), nullValue());
        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams().size(), equalTo(0));
        assertThat(config.getEnableFileCache(), equalTo(false));
        assertThat(config.getMaxOutputTriples(), nullValue());
        assertThat(config.getMetadataSources(), equalTo(Collections.<ConstructSourceConfig>emptyList()));
        assertThat(config.getSameAsSources().size(), equalTo(0));
        assertThat(config.getPrefixes(), equalTo(Collections.<String, String>emptyMap()));
        assertThat(config.getSeedResourceRestriction(), nullValue());
        assertThat(config.getPropertyResolutionStrategies(), equalTo(Collections.<URI, ResolutionStrategy>emptyMap()));
        assertThat(config.isLocalCopyProcessing(), equalTo(false));

        assertThat(config.getDataSources().size(), equalTo(1));
        DataSourceConfig dataSourceConfig = config.getDataSources().get(0);
        assertThat(dataSourceConfig.getName(), nullValue());
        assertThat(dataSourceConfig.getType(), equalTo(EnumDataSourceType.VIRTUOSO));
        assertThat(dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_HOST), equalTo("localhost"));
        assertThat(dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_PORT), equalTo("1111"));
        assertThat(dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_USERNAME), equalTo("dba"));
        assertThat(dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_PASSWORD), equalTo("dba2"));
        assertThat(dataSourceConfig.getNamedGraphRestriction(), notNullValue());
        assertThat(dataSourceConfig.getNamedGraphRestriction().getPattern(), equalTo(""));

        assertThat(config.getOutputs().size(), equalTo(1));
        Output output = config.getOutputs().get(0);
        assertThat(output.getType(), equalTo(EnumOutputType.FILE));
        assertThat(output.getDataContext(), nullValue());
        assertThat(output.getMetadataContext(), nullValue());
        assertThat(output.getName(), nullValue());
        assertThat(output.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH), equalTo("out.n3"));
        assertThat(output.getParams().get(ConfigParameters.DATA_SOURCE_FILE_FORMAT), equalTo("ntriples"));

        assertThat(config.getMaxDateDifference(), equalTo(ConfigConstants.MAX_DATE_DIFFERENCE));
        assertThat(config.getOutputConflictsOnly(), equalTo(false));
        assertThat(config.getOutputMappedSubjectsOnly(), equalTo(false));
        assertThat(config.getPreferredCanonicalURIs(), equalTo(ConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS));
        assertThat(config.getTempDirectory(), equalTo(ConfigConstants.TEMP_DIRECTORY));
        assertThat(config.getResultDataURIPrefix(), notNullValue());
        assertThat(config.getPublisherScoreWeight(), equalTo(ConfigConstants.PUBLISHER_SCORE_WEIGHT));
        assertThat(config.getAgreeCoefficient(), equalTo(ConfigConstants.AGREE_COEFFICIENT));
        assertThat(config.getQueryTimeout(), equalTo(ConfigConstants.DEFAULT_QUERY_TIMEOUT));
        assertThat(config.getScoreIfUnknown(), equalTo(ConfigConstants.SCORE_IF_UNKNOWN));
        assertThat(config.isProfilingOn(), equalTo(false));
        assertThat(config.getMaxFreeMemoryUsage(), equalTo(ConfigConstants.MAX_FREE_MEMORY_USAGE));
        assertThat(config.getMemoryLimit(), equalTo(null));
    }

    @Test
    public void parsesFullConfigFile() throws Exception {
        // Arrange
        File configFile = getResourceFile("/config/sample-config-full.xml");

        // Act
        Config config = ConfigReader.parseConfigXml(configFile);

        // Assert
        Map<String, String> expectedPrefixes = ImmutableMap.<String, String>builder()
                .put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                .put("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
                .put("xsd", "http://www.w3.org/2001/XMLSchema#")
                .put("owl", "http://www.w3.org/2002/07/owl#")
                .put("odcs", "http://opendata.cz/infrastructure/odcleanstore/")
                .build();
        assertThat(config.getPrefixes(), equalTo(expectedPrefixes));

        assertThat(config.getDataSources().size(), equalTo(3));
        assertThat(config.getDataSources().get(0).getName(), equalTo("virtuoso_local"));
        assertThat(config.getDataSources().get(0).getType(), equalTo(EnumDataSourceType.VIRTUOSO));
        assertThat(config.getDataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_HOST), equalTo("localhost"));
        assertThat(config.getDataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_PORT), equalTo("1111"));
        assertThat(config.getDataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_USERNAME), equalTo("dba"));
        assertThat(config.getDataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_VIRTUOSO_PASSWORD), equalTo("dba"));
        assertThat(config.getDataSources().get(0).getNamedGraphRestriction().getPattern(), equalTo(
                "{ SELECT ?g WHERE {?g odcs:metadataGraph ?m} }\n"
                        + "        UNION { SELECT ?g WHERE {?a odcs:attachedGraph ?g} }"
        ));
        assertThat(config.getDataSources().get(0).getNamedGraphRestriction().getVar(), equalTo("g"));

        assertThat(config.getDataSources().get(1).getName(), nullValue());
        assertThat(config.getDataSources().get(1).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getDataSources().get(1).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT), equalTo("http://localhost:8890/sparql"));
        assertThat(config.getDataSources().get(1).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL), equalTo("1000"));
        assertThat(config.getDataSources().get(1).getNamedGraphRestriction().getPattern(), equalTo(""));

        assertThat(config.getDataSources().get(2).getType(), equalTo(EnumDataSourceType.FILE));
        assertThat(config.getDataSources().get(2).getName(), nullValue());
        assertThat(config.getDataSources().get(2).getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH), equalTo("data.rdf"));
        assertThat(config.getDataSources().get(2).getParams().get(ConfigParameters.DATA_SOURCE_FILE_BASE_URI), equalTo("http://example.com"));
        assertThat(config.getDataSources().get(2).getParams().get(ConfigParameters.DATA_SOURCE_FILE_FORMAT), equalTo("ntriples"));
        assertThat(config.getDataSources().get(2).getNamedGraphRestriction().getPattern(), equalTo(""));

        assertThat(config.getMetadataSources().size(), equalTo(1));
        assertThat(config.getMetadataSources().get(0).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getMetadataSources().get(0).getName(), equalTo("metadata-local"));
        assertThat(config.getMetadataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT), equalTo("http://localhost:8890/sparql"));
        assertThat(config.getMetadataSources().get(0).getConstructQuery().trim(), equalTo("CONSTRUCT {?g odcs:score ?s } WHERE { ?g odcs:score ?s }"));

        assertThat(config.getSameAsSources().size(), equalTo(1));
        assertThat(config.getSameAsSources().get(0).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getSameAsSources().get(0).getName(), equalTo("sameAs-local"));
        assertThat(config.getSameAsSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT), equalTo("http://localhost:8890/sparql"));
        assertThat(config.getSameAsSources().get(0).getConstructQuery().trim(), equalTo("CONSTRUCT {?s owl:sameAs ?o} WHERE { ?s owl:sameAs ?o }"));

        assertThat(config.getCanonicalURIsInputFile(), equalTo(new File("output/canonicalUris.txt")));
        assertThat(config.getCanonicalURIsOutputFile(), equalTo(new File("output/canonicalUris2.txt")));
        assertThat(config.getMaxOutputTriples(), equalTo(999l));
        assertThat(config.getEnableFileCache(), equalTo(true));
        assertThat(config.isLocalCopyProcessing(), equalTo(true));
        assertThat(config.getSeedResourceRestriction().getVar(), equalTo("s"));
        assertThat(config.getSeedResourceRestriction().isTransitive(), equalTo(true));
        assertThat(config.getSeedResourceRestriction().getPattern(), equalTo("?s rdf:type <http://purl.org/procurement/public-contracts#Contract>"));

        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), equalTo("ALL"));
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), equalTo(EnumAggregationErrorStrategy.RETURN_ALL));
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), equalTo(EnumCardinality.MANYVALUED));

        assertThat(config.getPropertyResolutionStrategies().size(), equalTo(3));
        assertThat(config.getPropertyResolutionStrategies().get(new URIImpl("http://www.w3.org/2000/01/rdf-schema#label")).getResolutionFunctionName(),
                equalTo("BEST"));
        assertThat(config.getPropertyResolutionStrategies().get(new URIImpl("http://www.w3.org/2000/01/rdf-schema#label")).getParams(),
                equalTo((Map<String, String>)ImmutableMap.of("name", "value")));
        assertThat(config.getPropertyResolutionStrategies().get(new URIImpl("http://rdf.freebase.com/ns/location.geocode.longtitude")).getResolutionFunctionName(),
                equalTo("AVG"));
        assertThat(config.getPropertyResolutionStrategies().get(new URIImpl("http://rdf.freebase.com/ns/location.geocode.latitude")).getResolutionFunctionName(),
                equalTo("AVG"));
        assertThat(config.getPropertyResolutionStrategies().get(new URIImpl("http://rdf.freebase.com/ns/location.geocode.latitude")).getParams(),
                equalTo((Map<String, String>)ImmutableMap.<String, String>of()));

        assertThat(config.getOutputs().size(), equalTo(3));
        assertThat(config.getOutputs().get(0).getName(), equalTo("n3-output"));
        assertThat(config.getOutputs().get(0).getType(), equalTo(EnumOutputType.FILE));
        assertThat(config.getOutputs().get(0).getMetadataContext(), equalTo((URI) new URIImpl("http://opendata.cz/infrastructure/odcleanstore/metadata")));
        assertThat(config.getOutputs().get(0).getDataContext(), equalTo((URI) new URIImpl("http://opendata.cz/infrastructure/odcleanstore/resultxxx")));
        assertThat(config.getOutputs().get(0).getParams().size(), equalTo(6));

        assertThat(config.getOutputs().get(1).getName(), equalTo("virtuoso-output"));
        assertThat(config.getOutputs().get(1).getType(), equalTo(EnumOutputType.VIRTUOSO));
        assertThat(config.getOutputs().get(1).getMetadataContext(), nullValue());
        assertThat(config.getOutputs().get(1).getDataContext(), equalTo((URI) new URIImpl("http://opendata.cz/infrastructure/odcleanstore/result")));
        assertThat(config.getOutputs().get(1).getParams().size(), equalTo(5));

        assertThat(config.getOutputs().get(2).getName(), equalTo("sparql-output"));
        assertThat(config.getOutputs().get(2).getType(), equalTo(EnumOutputType.SPARQL));
        assertThat(config.getOutputs().get(2).getMetadataContext(), nullValue());
        assertThat(config.getOutputs().get(2).getDataContext(), equalTo((URI) new URIImpl("http://opendata.cz/infrastructure/odcleanstore/result")));
        assertThat(config.getOutputs().get(2).getParams().size(), equalTo(4));

        assertThat(config.getMaxDateDifference(), equalTo(ConfigConstants.MAX_DATE_DIFFERENCE));
        assertThat(config.getOutputConflictsOnly(), equalTo(false));
        assertThat(config.getOutputMappedSubjectsOnly(), equalTo(false));
        assertThat(config.getPreferredCanonicalURIs(), equalTo(ConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS));
        assertThat(config.getTempDirectory(), equalTo(ConfigConstants.TEMP_DIRECTORY));
        assertThat(config.getResultDataURIPrefix(), notNullValue());
        assertThat(config.getPublisherScoreWeight(), equalTo(ConfigConstants.PUBLISHER_SCORE_WEIGHT));
        assertThat(config.getAgreeCoefficient(), equalTo(ConfigConstants.AGREE_COEFFICIENT));
        assertThat(config.getQueryTimeout(), equalTo(ConfigConstants.DEFAULT_QUERY_TIMEOUT));
        assertThat(config.getScoreIfUnknown(), equalTo(ConfigConstants.SCORE_IF_UNKNOWN));
        assertThat(config.isProfilingOn(), equalTo(false));
        assertThat(config.getMaxFreeMemoryUsage(), equalTo(ConfigConstants.MAX_FREE_MEMORY_USAGE));
        assertThat(config.getMemoryLimit(), equalTo(null));
    }

    @Test
    public void parsesFullConfigFile2() throws Exception {
        // Arrange
        File configFile = getResourceFile("/config/sample-config-full2.xml");

        // Act
        Config config = ConfigReader.parseConfigXml(configFile);

        // Assert
        assertThat(config.getDataSources().size(), equalTo(2));
        assertThat(config.getDataSources().get(0).getName(), nullValue());
        assertThat(config.getDataSources().get(0).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getDataSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS), equalTo("100000"));

        assertThat(config.getDataSources().get(1).getName(), nullValue());
        assertThat(config.getDataSources().get(1).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getDataSources().get(1).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS), equalTo("100000"));

        assertThat(config.getSameAsSources().size(), equalTo(1));
        assertThat(config.getSameAsSources().get(0).getType(), equalTo(EnumDataSourceType.SPARQL));
        assertThat(config.getSameAsSources().get(0).getName(), nullValue());
        assertThat(config.getSameAsSources().get(0).getParams().get(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT), equalTo("http://localhost:8890/sparql"));
        assertThat(config.getSameAsSources().get(0).getConstructQuery().trim(), equalTo("CONSTRUCT {?s owl:sameAs ?o} WHERE { ?s owl:sameAs ?o }"));

        assertThat(config.getSeedResourceRestriction(), nullValue());

        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), equalTo("NONE"));
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams(),
                equalTo((Map<String, String>) ImmutableMap.of("name", "value")));

        assertThat(config.getOutputs().size(), equalTo(1));
        assertThat(config.getOutputs().get(0).getName(), nullValue());
        assertThat(config.getOutputs().get(0).getType(), equalTo(EnumOutputType.SPARQL));
        assertThat(config.getOutputs().get(0).getMetadataContext(), nullValue());
        assertThat(config.getOutputs().get(0).getDataContext(), nullValue());
        assertThat(config.getOutputs().get(0).getParams().size(), equalTo(3));
    }

    @Test(expected = InvalidInputException.class)
    public void throwsInvalidInputExceptionWhenInputFileInvalid() throws Exception {
        // Arrange
        File configFile = getResourceFile("/config/sample-config-invalid.xml");

        // Act
        ConfigReader.parseConfigXml(configFile);
    }

    private File getResourceFile(String resourcePath) {
        try {
            return new File(getClass().getResource(resourcePath).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}