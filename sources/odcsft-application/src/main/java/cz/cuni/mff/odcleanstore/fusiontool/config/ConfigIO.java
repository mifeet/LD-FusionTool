package cz.cuni.mff.odcleanstore.fusiontool.config;

import org.openrdf.model.URI;
import org.openrdf.rio.ParserConfig;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Configuration related to input and output.
 */
public interface ConfigIO {
    /**
     * List of data sources.
     * @return list of data sources.
     */
    List<DataSourceConfig> getDataSources();

    /**
     * List of owl:sameAs links sources.
     * @return list of data sources.
     */
    List<ConstructSourceConfig> getSameAsSources();

    /**
     * List of metadata sources.
     * @return list of data sources.
     */
    List<ConstructSourceConfig> getMetadataSources();

    /**
     * Prefix of named graphs and URIs where query results and metadata in the output are placed.
     * @return graph name prefix
     */
    String getResultDataURIPrefix();

    /**
     * List of result data outputs.
     * @return list of result data outputs
     */
    List<Output> getOutputs();

    /**
     * File where resolved canonical URIs shall be written.
     * Null means that canonical URIs will not be written anywhere.
     * @return file to write canonical URIs to or null
     */
    File getCanonicalURIsOutputFile();

    /**
     * File with list of preferred canonical URIs, one URI per line.
     * Null means no preferred URIs.
     * @return file with canonical URIs
     */
    File getCanonicalURIsInputFile();

    /**
     * Returns maximum number of triples allowed in the result.
     * Conflict Resolution should stop after the given number of triples is reached.
     * If null or less than zero, the output is unlimited.
     * @return maximum number of triples in the result or null for no limit
     */
    Long getMaxOutputTriples();

    /**
     * SPARQL restriction on URI resources which are initially loaded and processed.
     * If given, triples having matching resources and triples reachable from them are processed. All data
     * from matching input graphs are processed otherwise.
     * @return SPARQL restriction (group graph pattern) or null
     */
    SeedResourceRestriction getSeedResourceRestriction();

    ///**
    // * Indicates whether only conflict clusters with an actual conflict should be included in the output.
    // * @return true iff only conflict clusters with an actual conflict should be included in the output
    // */
    //boolean getOutputConflictsOnly();

    /**
     * Indicates whether only subjects for which an URI mapping exists should be included in the output.
     * @return true iff only subjects for which an URI mapping exists should be included in the output
     */
    boolean getOutputMappedSubjectsOnly();

    /**
     * Returns configuration for Sesame file parsers.
     * @return configuration for Sesame file parsers
     */
    ParserConfig getParserConfig();

    /**
     * Database queries timeout.
     * @return query timeout; zero means no timeout
     */
    Integer getQueryTimeout();

    /**
     * Returns true of profiling logs should be printed.
     * @return true iff profiling logs should be printed
     */
    boolean isProfilingOn();

    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();

    /**
     * Returns class to which resources included in the output must belong.
     * In other words, if this method returns X, only resources R for which a triple {@code R rdf:type X} exists are processed.
     * @return the required class of processed resources or null if all resources should be processed
     */
    URI getRequiredClassOfProcessedResources();
}
