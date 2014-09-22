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
    public static final String DATA_SOURCE_FILE_BASE_URI = "baseUri".toLowerCase();
    public static final String DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL = "minQueryInterval".toLowerCase();
    public static final String DATA_SOURCE_SPARQL_ENDPOINT = "endpointUrl".toLowerCase();
    public static final String DATA_SOURCE_VIRTUOSO_PASSWORD = "password";
    public static final String DATA_SOURCE_VIRTUOSO_USERNAME = "username";
    public static final String DATA_SOURCE_VIRTUOSO_PORT = "port";
    public static final String DATA_SOURCE_VIRTUOSO_HOST = "host";
    public static final String DATA_SOURCE_SPARQL_RESULT_MAX_ROWS = "sparqlResultMaxRows".toLowerCase();
    public static final String PROCESSING_CANONICAL_URI_OUTPUT_FILE = "canonicalUriOutputFile";
    public static final String PROCESSING_CANONICAL_URI_INPUT_FILE = "canonicalUriInputFile";
    public static final String PROCESSING_ENABLE_FILE_CACHE = "enableFileCache";
    public static final String PROCESSING_MAX_OUTPUT_TRIPLES = "maxOutputTriples";
    public static final String PROCESSING_LOCAL_COPY_PROCESSING = "localCopyProcessing";
    public static final String PROCESSING_ONLY_RESOURCES_WITH_CLASS = "processResourcesWithClass";
    public static final String OUTPUT_PATH = "path";
    public static final String OUTPUT_FORMAT = "format";
    public static final String OUTPUT_SPLIT_BY_MB = "splitByMb".toLowerCase();
    public static final String OUTPUT_DATA_CONTEXT = "dataContext".toLowerCase();
    public static final String OUTPUT_METADATA_CONTEXT = "metadataContext".toLowerCase();
    public static final String OUTPUT_HOST = "host";
    public static final String OUTPUT_PORT = "port";
    public static final String OUTPUT_USERNAME = "username";
    public static final String OUTPUT_PASSWORD = "password";
    public static final String OUTPUT_ENDPOINT_URL = "endpointUrl".toLowerCase();
    public static final String OUTPUT_SAME_AS_FILE = "sameAsFile".toLowerCase();

    /** Disable constructor for a utility class. */
    private ConfigParameters() {
    }


}
