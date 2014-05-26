/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.util.Map;

/**
 * Settings concerning inputs.
 * @author Jan Michelfeit
 */
public interface SourceConfig {
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
}
