/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.util.Map;

/**
 * Settings concerning SPARQL queries.
 * @author Jan Michelfeit
 */
public interface DataSourceConfig {
    /**
     * Returns type of this data source.
     * @return type of data source
     */
    EnumDataSourceType getType();
    
    /**
     * Returns a human-readable name of this data source.
     * @return human-readable name or null if unavailable
     */
    String getName();
    
    /**
     * Map of data source configuration parameters as map of parameter name->parameter value.
     * @return map of source configuration parameters
     */
    Map<String, String> getParams();
    
    /**
     * SPARQL group graph pattern limiting source payload named graphs. Must not be null.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();

    /**
     * SPARQL restriction on metadata named graphs. 
     * @return SPARQL restriction (group graph pattern) or null  
     */
    SparqlRestriction getMetadataGraphRestriction();
}
