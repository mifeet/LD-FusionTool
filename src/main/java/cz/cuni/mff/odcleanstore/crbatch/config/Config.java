package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;
import java.util.List;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;

/**
 * Encapsulation of CR-batch configuration.
 * @author Jan Michelfeit
 */
public interface Config extends QueryConfig {
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
     * Aggregation settings for conflict resolution.
     * URIs in the settings must have expanded namespace prefixes.
     * @return aggregation settings
     */
    AggregationSpec getAggregationSpec();
    
    /**
     * Indicates whether conflict resolution should be applied to all data at once (false),
     * or a separate output for each publisher should be created (true).
     * @return true if processing should be divided by publishers 
     */
    Boolean getProcessByPublishers();

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
     * SPARQL restriction on ontology named graphs.
     * @return SPARQL restriction (group graph pattern)  
     */
    SparqlRestriction getOntologyGraphRestriction();

    /**
     * SPARQL restriction on URI resources which are initially loaded and processed.
     * If given, triples having matching resources and triples reachable from them are processed. All data
     * from matching input graphs are processed otherwise.
     * @return SPARQL restriction (group graph pattern)  
     */
    SparqlRestriction getSeedResourceRestriction();
}