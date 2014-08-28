package cz.cuni.mff.odcleanstore.fusiontool.config;

import org.openrdf.model.URI;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Configuration related to processed data.
 */
public interface ConfigData {
    /**
     * Returns maximum number of triples allowed in the result.
     * Conflict Resolution should stop after the given number of triples is reached.
     * If null or less than zero, the output is unlimited.
     * @return maximum number of triples in the result or null for no limit
     */
    Long getMaxOutputTriples();

    ///**
    // * Indicates whether only conflict clusters with an actual conflict should be included in the output.
    // * @return true iff only conflict clusters with an actual conflict should be included in the output
    // */
    //boolean getOutputConflictsOnly();

    /**
     * Prefix of named graphs and URIs where query results and metadata in the output are placed.
     * @return graph name prefix
     */
    String getResultDataURIPrefix();

    /**
     * Indicates whether only subjects for which an URI mapping exists should be included in the output.
     * @return true iff only subjects for which an URI mapping exists should be included in the output
     */
    boolean getOutputMappedSubjectsOnly();

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
