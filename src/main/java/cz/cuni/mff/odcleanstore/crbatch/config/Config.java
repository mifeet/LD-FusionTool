package cz.cuni.mff.odcleanstore.crbatch.config;

import java.util.List;
import java.util.Map;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;

/**
 * Encapsulation of CR-batch configuration.
 * @author Jan Michelfeit
 */
public interface Config {
    /**
     * Virtuoso database connection string.
     * @return database connection string
     */
    String getDatabaseConnectionString();

    /**
     * Username for database connection.
     * @return username
     */
    String getDatabaseUsername(); 

    /**
     * Password for database connection.
     * @return password
     */
    String getDatabasePassword();
    
    /**
     * SPARQL group graph pattern limiting source payload named graphs.
     * Value of {@link #getNamedGraphRestrictionVar()} is the name of the SPARQL variable representing the payload graph.
     * @return SPARQL variable name
     */
    String getNamedGraphRestrictionPattern();
    
    /**
     * Variable representing named graphs in source named graph restriction pattern.
     * @return SPARQL variable name
     */
    String getNamedGraphRestrictionVar();

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
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();

    /**
     * Aggregation settings for conflict resolution.
     * @return aggregation settings
     */
    AggregationSpec getAggregationSpec();

    /**
     * Database queries timeout.
     * @return query timeout; zero means no timeout
     */
    Integer getQueryTimeout();

    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     * @return agree coefficient
     */
    Double getAgreeCoeficient();

    /**
     * Graph score used if none is given in the input.
     * @return default score
     */
    Double getScoreIfUnknown();

    /**
     * Weight of the named graph score.
     * @return named graph score weight
     */
    Double getNamedGraphScoreWeight();

    /**
     * Weight of the publisher score.
     * @return publisher score weight
     */
    Double getPublisherScoreWeight();

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * @return time interval in seconds
     */
    Long getMaxDateDifference();
}