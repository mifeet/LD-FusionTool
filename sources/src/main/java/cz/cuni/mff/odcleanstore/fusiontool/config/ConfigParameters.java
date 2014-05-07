/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Names of configuration parameters.
 */
public final class ConfigParameters {
    // CHECKSTYLE:OFF
    public static final String DATA_SOURCE_FILE_PATH = "path";
    public static final String DATA_SOURCE_FILE_FORMAT = "format";
    public static final String DATA_SOURCE_FILE_BASE_URI = "baseuri";
    public static final String DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL = "minqueryinterval";
    public static final String DATA_SOURCE_SPARQL_ENDPOINT = "endpointurl";
    public static final String DATA_SOURCE_VIRTUOSO_PASSWORD = "password";
    public static final String DATA_SOURCE_VIRTUOSO_USERNAME = "username";
    public static final String DATA_SOURCE_VIRTUOSO_PORT = "port";
    public static final String DATA_SOURCE_VIRTUOSO_HOST = "host";
    public static final String PROCESSING_CANONICAL_URI_OUTPUT_FILE = "canonicalUriOutputFile";
    public static final String PROCESSING_CANONICAL_URI_INPUT_FILE = "canonicalUriInputFile";
    public static final String PROCESSING_ENABLE_FILE_CACHE = "enableFileCache";
    public static final String PROCESSING_MAX_OUTPUT_TRIPLES = "maxOutputTriples";
    public static final String PROCESSING_SPARQL_RESULT_MAX_ROWS = "sparqlResultMaxRows";
    public static final String PROCESSING_LOCAL_COPY_PROCESSING = "localCopyProcessing";

    /** Disable constructor for a utility class. */
    private ConfigParameters() {
    }


}
