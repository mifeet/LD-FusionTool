/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.util.Map;

import org.openrdf.model.URI;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public interface Output {
    // CHECKSTYLE:OFF
    // Names of supported parameters
    public static String PATH_PARAM = "path";
    public static String FORMAT_PARAM = "format";
    public static String SPLIT_BY_MB_PARAM = "splitbymb";
    public static String DATA_CONTEXT_PARAM = "datacontext";
    public static String METADATA_CONTEXT_PARAM = "metadatacontext";
    public static String HOST_PARAM = "host";
    public static String PORT_PARAM = "port";
    public static String USERNAME_PARAM = "username";
    public static String PASSWORD_PARAM = "password";
    public static String ENDPOINT_URL_PARAM = "endpointurl";
    public static String SAME_AS_FILE_PARAM = "sameasfile";
    // CHECKSTYLE:ON
    
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
