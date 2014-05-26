/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import org.openrdf.model.URI;

import java.util.Map;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public interface Output {
    /**
     * Returns type of this output.
     * @return type of output
     */
    EnumOutputType getType();
    
    /**
     * Returns a human-readable name of this output.
     * @return human-readable name or null if unavailable
     */
    String getName();
    
    /**
     * Map of output configuration parameters as map of parameter name -> value.
     * @return map of output configuration parameters
     */
    Map<String, String> getParams();

    /**
     * Returns URI of named graph where resolved quads will be placed. 
     * Overrides the unique named graph assigned to each resolved quad from Conflict Resolution.
     * @return named graph URI
     */
    URI getDataContext();

    /**
     * Returns URI of named graph where Conflict Resolution metadata will be placed. 
     * @return named graph URI
     */
    URI getMetadataContext();
}
