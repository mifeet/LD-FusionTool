package cz.cuni.mff.odcleanstore.fusiontool.config;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import org.openrdf.model.URI;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Encapsulation of ODCS-FusionTool configuration.
 * @author Jan Michelfeit
 */
public interface Config {
    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();
    
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
     * Default conflict resolution strategy.
     * @return resolution strategy
     */
    ResolutionStrategy getDefaultResolutionStrategy();
    
    /**
     * Conflicts resolution strategy settings for individual properties.
     * Key is the property URI (must have expanded namespace prefix), value the actual strategy.
     * @return map of resolution strategies indexed by property URIs 
     */
    Map<URI, ResolutionStrategy> getPropertyResolutionStrategies();
    
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
    Double getAgreeCoefficient();

    /**
     * Graph score used if none is given in the input.
     * @return default score
     */
    Double getScoreIfUnknown();

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
     * Indicates whether disable (true) or enable (false) file cache for objects that needed by CR algorithm
     * that may not fit into the memory.
     * @return whether to disable algorithm file cache
     */
    boolean getEnableFileCache(); // TODO: detect automatically whether it's neccessary
    
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
    
    /**
     * Returns a default set of preferred URIs. 
     * These are added to preferred URIs obtained from configuration and canonical URI file.
     * @return set of preferred canonical URIs
     */
    Collection<String> getPreferredCanonicalURIs();
    
    /**
     * Returns true of profiling logs should be printed.
     * @return true iff profiling logs should be printed
     */
    boolean isProfilingOn();

    /**
     * Indicates whether only conflict clusters with an actual conflict should be included in the output.
     * @return true iff only conflict clusters with an actual conflict should be included in the output
     */
    boolean getOutputConflictsOnly();

    /**
     * Indicates whether only subjects for which an URI mapping exists should be included in the output.
     * @return true iff only subjects for which an URI mapping exists should be included in the output
     */
    boolean getOutputMappedSubjectsOnly();

    /**
     * Directory for temporary files.
     * @return directory for temporary files.
     */
    File getTempDirectory();

    /**
     * Maximum number of rows to be requested in any SPARQL query.
     * @return maximum number of rows to be requested
     */
    int getSparqlResultMaxRows();

    /**
     * Indicates whether data should be downloaded to a local file prior to processing.
     * @return true iff data should be pre-downloaded to a local file
     */
    boolean isLocalCopyProcessing();


    /**
     * Maximum memory amount to use for large operations.
     * Null means no limit.
     * If the limit is too high, it may cause OutOfMemory exceptions.
     * @return memory limit in bytes or null for no limit
     */
    Long getMemoryLimit();

    /**
     * Max portion of free memory to use.
     */
    float getMaxFreeMemoryUsage();
}